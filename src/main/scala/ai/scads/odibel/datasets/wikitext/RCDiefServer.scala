package ai.scads.odibel.datasets.wikitext

import org.apache.hadoop.shaded.org.apache.http.client.config.RequestConfig
import org.apache.hadoop.shaded.org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.hadoop.shaded.org.apache.http.entity.StringEntity
import org.apache.hadoop.shaded.org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.hadoop.shaded.org.apache.http.message.BasicHeader
import org.apache.hadoop.shaded.org.apache.http.util.EntityUtils

import java.io.IOException
import java.nio.charset.StandardCharsets
import org.slf4j.LoggerFactory

/**
 * Handles the DBpedia triple extraction using the DIEF server extract HTTP API
 * @param endpoint URL of the DIEF server endpoint
 */
class RCDiefServer(endpoint: String) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val requestConfig = RequestConfig.custom()
    .setConnectTimeout(5000)    // Adjust as necessary
    .setSocketTimeout(10000)    // Adjust as necessary
    .build()

  private val client: CloseableHttpClient = HttpClientBuilder.create()
    .setDefaultRequestConfig(requestConfig)
    .build()

  def extract(xml: String): Either[String,Exception] = {
    val maxRetries = 3
    var attempt = 0
    var success = false
    var lastException: Exception = null
    var responseContent: String = null

    while (attempt < maxRetries && !success) {
      attempt += 1
      var response: CloseableHttpResponse = null
      try {
        val entity = new StringEntity(xml, StandardCharsets.UTF_8)
        val request = new HttpPost(s"$endpoint/extract?format=n-triples")
        request.setEntity(entity)
        request.setHeader(new BasicHeader("Content-Type", "application/xml"))

//        logger.info(s"Attempt $attempt: Sending request to $url")
        response = client.execute(request)
        val statusCode = response.getStatusLine.getStatusCode

        if (statusCode >= 200 && statusCode < 300) {
          val responseEntity = response.getEntity
          responseContent = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8)
          EntityUtils.consume(responseEntity)
//          logger.info(s"Attempt $attempt: Received successful response from $endpoint")
          success = true
        } else {
          // Handle non-successful status codes
          logger.error(s"Attempt $attempt: Received status code $statusCode from $endpoint")
          lastException = new IOException(s"Unexpected response status: $statusCode")
          val responseEntity = response.getEntity
          responseContent = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8)
        }
      } catch {
        case e: IOException =>
          logger.error(s"Attempt $attempt: I/O error during request to $endpoint", e)
          lastException = e
        case e: Exception =>
          logger.error(s"Attempt $attempt: Unexpected error during request to $endpoint", e)
          lastException = e
      } finally {
        if (response != null) {
          try {
            response.close()
          } catch {
            case e: IOException =>
              logger.error("Error closing response", e)
          }
        }
      }

      if (!success && attempt < maxRetries) {
        logger.info(s"Attempt $attempt failed. Retrying after delay...")
        Thread.sleep(2000) // Wait for 2 seconds before retrying
      }
    }

    if (!success) {
      logger.error(s"All $maxRetries attempts failed.")
      logger.error(responseContent)
      Right(lastException)
    } else {
      Left(responseContent)
    }
  }
}
