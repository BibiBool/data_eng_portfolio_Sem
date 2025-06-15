# Data Dictionary - Raw Log Data

This document provides a detailed description of the schema and columns for the raw log files ingested from the S3 bucket. These logs typically originate from a content delivery network (CDN) or web server, capturing details about user requests and server responses.

## Table: `raw_cdn_logs`

* **Description:** Stores the raw, unprocessed access log entries from the S3 bucket. Each row represents a single request/response event captured by the CDN. This table serves as the initial landing zone for all log data before any transformations or aggregations.
* **Source:** S3 Bucket (raw log files, e.g., CloudFront access logs or similar).
* **Load Frequency:** Daily (or as frequently as log files are delivered to S3).
* **Key Metrics/Business Rules:**
    * Foundation for all subsequent analytics related to website traffic, user behavior, and CDN performance.
    * Used for security analysis and troubleshooting.

### Columns:

#### `date`

* **Description:** The date of the request in YYYY-MM-DD format.
* **Data Type:** `STRING` (will be cast to `DATE` in processed layers)
* **Constraints:** N/A (nullable in raw, but typically always present)
* **Example Values:** `2024-05-20`, `2024-05-21`
* **Transformation Logic:** Raw string from log; can be parsed to a `DATE` type.

#### `time`

* **Description:** The time of the request in HH:MM:SS format (UTC).
* **Data Type:** `STRING` (will be combined with `date` to form a `TIMESTAMP` in processed layers)
* **Constraints:** N/A (nullable in raw, but typically always present)
* **Example Values:** `14:30:00`, `09:15:23`
* **Transformation Logic:** Raw string from log; can be combined with `date` and parsed to a `TIMESTAMP` type.

#### `x_edge_location`

* **Description:** The edge location (e.g., CDN POP) that served the request. This indicates the geographical location of the server that responded to the user.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `LHR50-C1`, `SFO5-C3`, `CDG52-C2`
* **Transformation Logic:** Raw string from log.

#### `sc_bytes`

* **Description:** The number of bytes returned to the viewer for the request. This represents the size of the response.
* **Data Type:** `STRING` (will be cast to `BIGINT` or `LONG` in processed layers)
* **Constraints:** N/A
* **Example Values:** `12345`, `567`, `890123`
* **Transformation Logic:** Raw string from log; can be parsed to a numeric type.

#### `c_ip`

* **Description:** The IP address of the client (viewer) that made the request.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `203.0.113.45`, `198.51.100.10`
* **Transformation Logic:** Raw string from log. Potentially sensitive data.

#### `cs_method`

* **Description:** The request method (e.g., GET, POST, PUT, HEAD).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `GET`, `POST`, `HEAD`
* **Transformation Logic:** Raw string from log.

#### `cs_Host`

* **Description:** The domain name of the resource requested by the client (e.g., `www.example.com`).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `www.yourwebsite.com`, `images.yourwebsite.com`
* **Transformation Logic:** Raw string from log.

#### `cs_uri_stem`

* **Description:** The stem of the URI (the path without the query string) that was requested by the client.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `/index.html`, `/images/logo.png`, `/products/item123`
* **Transformation Logic:** Raw string from log.

#### `sc_status`

* **Description:** The HTTP status code returned to the client (e.g., 200 for OK, 404 for Not Found).
* **Data Type:** `STRING` (will be cast to `INT` in processed layers)
* **Constraints:** N/A
* **Example Values:** `200`, `404`, `301`, `500`
* **Transformation Logic:** Raw string from log; can be parsed to an integer type.

#### `cs_Referer`

* **Description:** The Referer header in the request, indicating the URL of the page that linked to the requested resource.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `https://www.google.com/`, `https://www.yourwebsite.com/previous-page`
* **Transformation Logic:** Raw string from log. Often empty or "-" if no referer.

#### `cs_User_Agent`

* **Description:** The User-Agent header in the request, identifying the client's browser, operating system, and device.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36`, `curl/7.81.0`
* **Transformation Logic:** Raw string from log. Useful for browser/device analytics.

#### `cs_uri_query`

* **Description:** The query string portion of the URI (e.g., `param1=value1&param2=value2`).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `?id=123&category=books`, `?search=data+engineering`
* **Transformation Logic:** Raw string from log. Often empty or "-" if no query.

#### `cs_Cookie`

* **Description:** The Cookie header in the request. Contains cookies sent by the client.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `PHPSESSID=abcdef12345; user_id=987`, `-`
* **Transformation Logic:** Raw string from log. Often empty or "-". Potentially sensitive data.

#### `x_edge_result_type`

* **Description:** The result of the request from the perspective of the edge location (e.g., Hit, Miss, Error).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `Hit`, `Miss`, `Error`, `LimitExceeded`
* **Transformation Logic:** Raw string from log.

#### `x_edge_request_id`

* **Description:** A unique ID for the request generated by the CDN edge location. Useful for tracing individual requests.
* **Data Type:** `STRING`
* **Constraints:** Unique per request.
* **Example Values:** `M_A-1234567890abcdefghijklmnopqrs_F_a-BCDEFG`
* **Transformation Logic:** Raw string from log.

#### `x_host_header`

* **Description:** The Host header that was received by the CDN edge server.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `www.yourwebsite.com`, `cdn.yourwebsite.com`
* **Transformation Logic:** Raw string from log.

#### `cs_protocol`

* **Description:** The protocol used for the request (e.g., HTTP, HTTPS).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `HTTP`, `HTTPS`
* **Transformation Logic:** Raw string from log.

#### `cs_bytes`

* **Description:** The number of bytes that the client sent to the CDN edge location for the request (e.g., for POST requests).
* **Data Type:** `STRING` (will be cast to `BIGINT` or `LONG` in processed layers)
* **Constraints:** N/A
* **Example Values:** `0`, `500`, `10240`
* **Transformation Logic:** Raw string from log; can be parsed to a numeric type.

#### `time_taken`

* **Description:** The total time, in seconds, from when the CDN edge location received the request until it returned the last byte of the response to the viewer.
* **Data Type:** `STRING` (will be cast to `DOUBLE` or `FLOAT` in processed layers)
* **Constraints:** N/A
* **Example Values:** `0.005`, `1.234`, `0.5`
* **Transformation Logic:** Raw string from log; can be parsed to a numeric type.

#### `x_forwarded_for`

* **Description:** The value of the `X-Forwarded-For` header, often used to identify the originating IP address of a client connecting to a web server through an HTTP proxy or load balancer.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `192.0.2.1`, `203.0.113.10, 198.51.100.5`
* **Transformation Logic:** Raw string from log. Often empty or "-".

#### `ssl_protocol`

* **Description:** The SSL/TLS protocol version used for the request (e.g., TLSv1.2, TLSv1.3).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `TLSv1.2`, `TLSv1.3`, `-`
* **Transformation Logic:** Raw string from log. Only present for HTTPS requests.

#### `ssl_cipher`

* **Description:** The SSL/TLS cipher used for the request (e.g., ECDHE-RSA-AES128-GCM-SHA256).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `ECDHE-RSA-AES128-GCM-SHA256`, `AES256-SHA`, `-`
* **Transformation Logic:** Raw string from log. Only present for HTTPS requests.

#### `x_edge_response_result_type`

* **Description:** The result of the response processing at the CDN edge (e.g., `Priced`, `Error`, `CapacityExceeded`).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `Priced`, `Error`, `LimitExceeded`
* **Transformation Logic:** Raw string from log.

#### `cs_protocol_version`

* **Description:** The version of the client protocol (e.g., HTTP/1.1, HTTP/2).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `HTTP/1.1`, `HTTP/2`
* **Transformation Logic:** Raw string from log.

#### `fle_status`

* **Description:** Field-level encryption status (e.g., success, error, not-applied).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `Success`, `Error`, `NotApplied`
* **Transformation Logic:** Raw string from log. Specific to CDN features like CloudFront Field-Level Encryption.

#### `fle_encrypted_fields`

* **Description:** A list of fields that were encrypted by field-level encryption.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `header.cookie.PHPSESSID`, `-`
* **Transformation Logic:** Raw string from log. Specific to CDN features like CloudFront Field-Level Encryption.

#### `c_port`

* **Description:** The port number used by the client for the request.
* **Data Type:** `STRING` (will be cast to `INT` in processed layers)
* **Constraints:** N/A
* **Example Values:** `443`, `80`
* **Transformation Logic:** Raw string from log; can be parsed to an integer type.

#### `time_to_first_byte`

* **Description:** The time, in seconds, from when the CDN edge location received the request until it returned the first byte of the response to the viewer.
* **Data Type:** `STRING` (will be cast to `DOUBLE` or `FLOAT` in processed layers)
* **Constraints:** N/A
* **Example Values:** `0.001`, `0.123`
* **Transformation Logic:** Raw string from log; can be parsed to a numeric type.

#### `x_edge_detailed_result_type`

* **Description:** A more detailed result type for the request from the CDN edge, providing more granular information than `x_edge_result_type`.
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `Hit`, `Miss`, `Error`, `LambdaFunctionError`
* **Transformation Logic:** Raw string from log.

#### `sc_content_type`

* **Description:** The `Content-Type` header of the response (e.g., `text/html`, `image/jpeg`, `application/json`).
* **Data Type:** `STRING`
* **Constraints:** N/A
* **Example Values:** `text/html`, `image/png`, `application/json`
* **Transformation Logic:** Raw string from log.

#### `sc_content_len`

* **Description:** The `Content-Length` header of the response, indicating the size of the body in bytes.
* **Data Type:** `STRING` (will be cast to `BIGINT` or `LONG` in processed layers)
* **Constraints:** N/A
* **Example Values:** `1024`, `50000`, `-`
* **Transformation Logic:** Raw string from log; can be parsed to a numeric type. Often `-` if not applicable (e.g., for HEAD requests).

#### `sc_range_start`

* **Description:** The start of the byte range requested by the client for partial content requests.
* **Data Type:** `STRING` (will be cast to `BIGINT` or `LONG` in processed layers)
* **Constraints:** N/A
* **Example Values:** `0`, `1024`, `-`
* **Transformation Logic:** Raw string from log. Present only for range requests.

#### `sc_range_end`

* **Description:** The end of the byte range requested by the client for partial content requests.
* **Data Type:** `STRING` (will be cast to `BIGINT` or `LONG` in processed layers)
* **Constraints:** N/A
* **Example Values:** `1023`, `2047`, `-`
* **Transformation Logic:** Raw string from log. Present only for range requests.