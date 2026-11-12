package gears.io.http

import java.nio.file.Files

/** A self-signed certificate for `localhost` and 127.0.0.1, valid for a century, and its key, written to files once. */
object TestCertificate {
  val certificatePem: String = """-----BEGIN CERTIFICATE-----
MIIBnDCCAUGgAwIBAgIUYzbV+EV1qNcHIo1Atxg1pEC4IgkwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAyOTIyMzM0NFoYDzIxMjYxMDA1
MjIzMzQ0WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAQhruvIzDUdaskLJ4VJ3/tq9qJ15QR+jju1D7JVcEkvjxQXCX+pGE+c
niYrD1S9riF6+P6JMc/2grHRSITmlNHCo28wbTAdBgNVHQ4EFgQUhkI6jOyYTalm
0r140EMyzU1+6JgwHwYDVR0jBBgwFoAUhkI6jOyYTalm0r140EMyzU1+6JgwDwYD
VR0TAQH/BAUwAwEB/zAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwCgYIKoZI
zj0EAwIDSQAwRgIhAPCMibGP6LrWT0GHipaTHzqsP4xOuljGxw5dmLr49Oh7AiEA
7Q6t/ua09dOBbx0+gM3xIXtAzSfHGQDbjFFddA8Qucc=
-----END CERTIFICATE-----
"""

  val keyPem: String = """-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgCWtI7BMr5wFX2ocs
fjfnS52UOeDe7hL7lE3NqAHc2yGhRANCAAQhruvIzDUdaskLJ4VJ3/tq9qJ15QR+
jju1D7JVcEkvjxQXCX+pGE+cniYrD1S9riF6+P6JMc/2grHRSITmlNHC
-----END PRIVATE KEY-----
"""

  private lazy val files: (String, String) = {
    val dir = Files.createTempDirectory("gears-http-tls")
    val c = dir.resolve("cert.pem")
    val k = dir.resolve("key.pem")
    Files.writeString(c, certificatePem)
    Files.writeString(k, keyPem)
    (c.toString, k.toString)
  }

  def certificate: String = files._1
  def key: String = files._2
}
