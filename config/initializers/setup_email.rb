ActionMailer::Base.smtp_settings = {
  :address              => "smtp.gmail.com",
  :port                 => 587,
  :domain               => Rails.application.config.CONTACT_DOMAIN,
  :user_name            => Rails.application.config.CONTACT_EMAIL,
  :password             => Rails.application.config.CONTACT_PASSWORD,
  :authentication       => "plain",
  :enable_starttls_auto => true
}
