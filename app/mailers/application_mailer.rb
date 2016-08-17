class ApplicationMailer < ActionMailer::Base
  default from: 'no-reply@vt.edu', to: 'geodata@vt.edu'
  layout 'mailer'
end