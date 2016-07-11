class IssueTypes
  require 'yaml'

  def self.load
    issue_type_file = File.join(Rails.root, 'config', 'issue_types.yml')
    issue_types = YAML::load_file(issue_type_file) if File.exists?(issue_type_file)
    issue_types.map do |type|
      [type, type]
    end
  end

end
