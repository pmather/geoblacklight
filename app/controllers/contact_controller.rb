class ContactController < ApplicationController
  
  def form
    @issue_types = Rails.application.config.issue_types
  end

  def submit
    ContactMailer.new_contact(params['name'], params['email'], params['subject'], params['category'], params['message']).deliver_later
    flash[:success] = "Your message has been sent. Thank you for contacting #{t('blacklight.application_name')}."
    redirect_to contact_form_path
  end

end