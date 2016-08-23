# There are three separate scripts used in migrating comments from the OSF to Discourse
# The first script is in the OSF, it can be run as
# (1) python -m scripts.migration.migrate_to_discourse export_file
# This file can then be imported to Discourse with
# (2) bundle exec ruby script/import_scripts/osf.rb export_file return_file
# which will create all of the users, categories, groups/projects, and topics
# that were exported from the osf. The return file contains id numbers for these
# various entities that the OSF will need to refer to them. These id numbers
# are then reimported back into the OSF with
# (3) python -m scripts.migration.migrate_from_discourse return_file
# Because the osf.rb import script does not exist in the actual discourse docker container
# The script will have to be manually added into script/import_scripts directory before executing

if ARGV.length != 2 || !File.exists?(ARGV[0])
    STDERR.puts 'Usage of OSF importer:', 'bundle exec ruby osf.rb <path to file to import from osf> <path to file to export to osf>'
    STDERR.puts 'Make sure the import file exists' if ARGV.length >= 1 && !File.exists?(ARGV[0])
    exit 1
end

require File.expand_path(File.dirname(__FILE__) + "/base.rb")

require 'json'
require 'pry'

class ImportScripts::Osf < ImportScripts::Base
    BATCH_SIZE = 1000
    CATEGORY_COLORS = ['BF1E2E', '3AB54A', '652D90']

    def initialize
        super
    end

    def import_objects(objects, object_type, total_count, offset, file_out)
        if object_type == 'user'
            import_users(objects, total_count, offset, file_out)
        elsif object_type == 'project'
            import_groups(objects, total_count, offset, file_out)
        elsif object_type == 'post'
            import_posts(objects, total_count, offset, file_out)
        end
    end

    def execute
        puts "Removing SSO Records because they might conflict with added users."
        SingleSignOnRecord.delete_all()

        import_categories

        objects = []
        object_type = nil
        object_total_count = 0
        offset = 0

        file_in = File.new(ARGV[0], 'r')
        file_out = File.new(ARGV[1], 'w')

        file_in.each_line do |line|
            obj = JSON::parse(line)
            if object_type && (obj['type'] == 'count' || objects.length >= BATCH_SIZE)
                import_objects(objects, object_type, object_total_count, offset, file_out)
                offset += objects.length
                objects = []
            end
            if obj['type'] == 'count'
                object_type = obj['object_type']
                object_total_count = obj['count']
                offset = 0
            else
                objects << obj
            end
        end

        import_objects(objects, object_type, object_total_count, offset, file_out) if objects.length > 0

        file_in.close
        file_out.close
    end

    def import_categories
        puts "", "importing categories..."
        create_categories([0, 1, 2]) do |i|
            {
                id: ["files", "wiki", "nodes"][i], # These need to match the .target_type field on the OSF objects
                name: ["Files", "Wikis", "Projects"][i],
                color: CATEGORY_COLORS[i]
            }
        end
    end

    def import_users(users, total_count, offset, file_out)
        puts '', "creating users"
        create_users(users, total: total_count, offset: offset) do |user|
            # Avoid the expensive check if the user has already been imported
            next if find_user_by_import_id(user['username'].to_i(36))
            # We need to check if the user with the same email already exists before asking for a new one to be created.
            # If we don't we will run into problems later down the line.
            user_old = User.find_by_email(user['email'])
            if user_old
                user_old.custom_fields['import_id'] = user['username'].to_i(36)
                user_old.custom_fields['is_disabled'] = user['is_disabled']
                user_old.save
                puts "Skipped creating user w/ email #{user['email']}, they already exist. Merging with imported user."
                next
            end
            {
                id: user['username'].to_i(36),
                email: user['email'],
                username: user['username'],
                name: user['name'],
                avatar_url: user['avatar_url'],
                custom_fields: {
                    is_disabled: user['is_disabled'],
                },
            }
        end
        users.each do |user_info|
            user = find_user_by_import_id(user_info['username'].to_i(36))
            if user == nil
                raise "It seems that the database has more than one user with email #{user_info['email']}. Please correct this before continuing."
            end
            raise "is_disabled failed to import, is: #{user.custom_fields['is_disabled']}" unless (user.custom_fields['is_disabled'] == 't') == user_info['is_disabled']

            # Upload the avatar if needed
            unless user.has_uploaded_avatar
                UserAvatar.import_url_for_user(user.custom_fields['import_avatar_url'], user)
                user.save
            end

            file_out.write({
                type: 'user',
                guid: user_info['username'],
                user_id: user.id,
            }.to_json)
            file_out.write("\n")
        end
    end

    def import_groups(projects, total_count, offset, file_out)
        puts '', "creating groups"
        create_groups(projects, total: total_count, offset: offset) do |project|
            {
                id: project['guid'].to_i(36),
                name: project['guid'],
                visible: project['is_public'],
                custom_fields: {
                    is_deleted: project['is_deleted'],
                },
            }
        end
        projects.each do |project|
            group = find_group_by_import_id(project['guid'].to_i(36))
            group.bulk_add(project['contributors'].map { |u| user_id_from_imported_user_id(u.to_i(36)) } )
            group.save
            raise "Visibility failed to import to group: " unless group.visible == project['is_public']
            raise "is_deleted failed to import, is: #{group.custom_fields['is_deleted']}" unless (group.custom_fields['is_deleted'] == 't') == project['is_deleted']

            file_out.write({
                type: 'project',
                guid: project['guid'],
                group_id: group.id,
                group_public: project['is_public'],
                group_users: project['contributors'],
            }.to_json)
            file_out.write("\n")
        end
    end

    def convertMentions(postContent)
        postContent.gsub(/\[[@|\+].*?(?<!\\)\]\(https?:\/\/[a-z\d:.]+?\/([a-z\d]{5})\/\)/, '@\1')
    end

    def import_posts(posts, total_count, offset, file_out)
        puts "", "creating topics and posts"
        @posts_hash ||= Hash.new
        @posts_hash.merge! posts.map {|p| [p['topic_guid'] || p['comment_guid'], p]}.to_h

        post_results = create_posts(posts, total: total_count, offset: offset) do |post|
            parent_post = post
            while parent_post['post_type'] == 'comment'
                parent_guid = parent_post['reply_to']
                parent_post = @posts_hash[parent_guid]
                if parent_post == nil
                    puts "Comment #{post['comment_guid']} skipped because parent #{parent_guid} does not exist."
                    break
                end
            end
            next if parent_post == nil

            project_guid = parent_post['parent_guids'][0]
            project_group = find_group_by_import_id(project_guid.to_i(36))
            project_deleted = 't' == project_group.custom_fields['is_deleted']

            converted_content = convertMentions(post['content'])

            if post['post_type'] == 'topic'
                {
                    id: post['topic_guid'].to_i(36),
                    title: post['title'],
                    raw: converted_content,
                    user_id: -1, #system
                    created_at: Time.parse(post['date_created']),
                    category: category_id_from_imported_category_id(post['type']),
                    custom_fields: {
                        is_deleted: post['is_deleted'],
                    },
                    deleted_at: post['is_deleted'] || project_deleted ? Time.new : nil,
                }
            else
                parent = topic_lookup_from_imported_post_id(post['reply_to'].to_i(36))
                {
                    id: post['comment_guid'].to_i(36),
                    raw: converted_content,
                    user_id: user_id_from_imported_user_id(post['user'].to_i(36)),
                    topic_id: parent[:topic_id],
                    reply_to_post_number: parent[:post_number],
                    created_at: Time.parse(post['date_created']),
                    custom_fields: {
                        is_deleted: post['is_deleted'],
                    },
                    deleted_at: post['is_deleted'] || project_deleted ? Time.new : nil,
                }
            end
        end
        puts "Created #{post_results[0]} posts and skipped #{post_results[1]} already created posts"

        posts.each do |post_data|
            next unless post_data['post_type'] == 'topic'
            topic_data = topic_lookup_from_imported_post_id(post_data['topic_guid'].to_i(36))

            topic = Topic.find(topic_data[:topic_id])
            topic.custom_fields['parent_guids'] = "-#{post_data['parent_guids'].join('-')}-"
            topic.custom_fields['project_guid'] = post_data['parent_guids'][0]
            topic.custom_fields['topic_guid'] = post_data['topic_guid']
            topic.save

            parent_guids = topic.custom_fields['parent_guids'].split('-').delete_if { |e| e.length == 0 }
            project_guid = topic.custom_fields['project_guid']
            topic_guid = topic.custom_fields['topic_guid']
            raise "Parent guids did not persist, #{parent_guids} != #{post_data['parent_guids']}" unless parent_guids == post_data['parent_guids']
            raise "Project guid did not persist" unless project_guid == post_data['parent_guids'][0]
            raise "Topic guid did not persist" unless topic_guid == post_data['topic_guid']
            raise "Topic category did not persist" unless ["Files", "Wikis", "Projects"].include? topic.category.name

            file_out.write({
                type: 'topic',
                guid: topic_guid,
                topic_id: topic.id,
                topic_title: topic.title,
                topic_parent_guids: parent_guids,
                topic_deleted: topic.deleted_at != nil,
                post_id: post_id_from_imported_post_id(post_data['topic_guid'].to_i(36)),
            }.to_json)
            file_out.write("\n")
        end

    end

end

ImportScripts::Osf.new.perform
