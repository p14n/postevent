# Create a private key for the certificate
resource "tls_private_key" "postevent" {
  algorithm = "RSA"
}

# Create a self-signed certificate
resource "tls_self_signed_cert" "postevent" {
  private_key_pem = tls_private_key.postevent.private_key_pem

  subject {
    common_name  = "postevent.internal"
    organization = "Postevent Internal"
  }

  validity_period_hours = 87600 # 10 years

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]
}

# Upload the certificate to ACM
resource "aws_acm_certificate" "postevent" {
  private_key      = tls_private_key.postevent.private_key_pem
  certificate_body = tls_self_signed_cert.postevent.cert_pem

  tags = {
    Name = "postevent-internal"
  }

  lifecycle {
    create_before_destroy = true
  }
}
