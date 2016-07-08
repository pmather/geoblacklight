class ContactController < ApplicationController
  
  def form
    @issue_types = issue_types
  end

  def submit
    ContactMailer.new_contact(params['name'], params['email'], params['subject'], params['category'], params['message']).deliver_later
    flash[:success] = "Your message has been sent. Thank you for contacting #{t('blacklight.application_name')}."
    @issue_types = issue_types
    render 'form'
  end

  private

  def issue_types
    return [
      ["Depositing content", "Depositing content"],
      ["Making changes to my content", "Making changes to my content"],
      ["Browsing and searching", "Browsing and searching"],
      ["Reporting a problem", "Reporting a problem"],
      ["General inquiry or request", "General inquiry or request"]
    ].freeze
  end
end
