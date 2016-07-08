class ContactMailer < ApplicationMailer
  def new_contact name, email, subject, category, message
    @name = name
    @email = email
    @subject = subject
    @category = category
    @message = message
    mail(to: Rails.application.config.CONTACT_EMAIL, from: @email, subject: subject)    
  end
end
