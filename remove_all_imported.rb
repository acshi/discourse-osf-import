require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::RemoveAllImported < ImportScripts::Base
    def initialize
        super
    end

    def execute
        puts "", "Removing imported posts..."
        imported_posts = Post.unscoped
            .joins("LEFT JOIN post_custom_fields AS cf ON (posts.id = cf.post_id)")
            .references('cf')
            .where("cf.name = 'import_id'")
        imported_post_ids = imported_posts.select(:id).to_a
        imported_topic_ids = imported_posts.select(:topic_id).to_a
        imported_posts.delete_all
        PostCustomField.delete_all(post_id: imported_post_ids)
        puts "Removed #{imported_post_ids.length} posts"

        puts "", "Removing imported topics..."
        Topic.delete_all(id: imported_topic_ids)
        TopicCustomField.delete_all(topic_id: imported_topic_ids)
        puts "Removed #{imported_topic_ids.length} topics"

        puts "", "Removing imported groups..."
        imported_groups = Group.unscoped
            .joins("LEFT JOIN group_custom_fields AS cf ON (groups.id = cf.group_id)")
            .references('cf')
            .where("cf.name = 'import_id'")
        imported_group_ids = imported_groups.select(:id).to_a
        imported_groups.delete_all
        GroupCustomField.delete_all(group_id: imported_group_ids)
        puts "Removed #{imported_group_ids.length} groups"

        puts "", "Removing imported categories..."
        imported_categories = Category.unscoped
            .joins("LEFT JOIN category_custom_fields AS cf ON (categories.id = cf.category_id)")
            .references('cf')
            .where("cf.name = 'import_id'")
        imported_category_ids = imported_categories.select(:id).to_a
        imported_categories.delete_all
        CategoryCustomField.delete_all(category_id: imported_category_ids)
        puts "Removed #{imported_category_ids.length} categories", ""

        puts "", "Removing imported users..."
        imported_users = User.unscoped
            .joins("LEFT JOIN user_custom_fields AS cf ON (users.id = cf.user_id)")
            .references('cf')
            .where("cf.name = 'import_id'")
        imported_user_ids = imported_users.select(:id).to_a
        imported_users.delete_all
        UserCustomField.delete_all(user_id: imported_user_ids)
        puts "Removed #{imported_user_ids.length} users"

        puts "", "Removing SSO records..."
        SingleSignOnRecord.delete_all(user_id: imported_user_ids)
        puts "Done!"
    end

end

ImportScripts::RemoveAllImported.new.perform
