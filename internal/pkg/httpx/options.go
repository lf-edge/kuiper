package httpx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// HTTPRequestOptions using for customized http request.
type HTTPRequestOptions func(req *http.Request) error

const (
	acceptEncoding  = "Accept-Encoding"
	contentEncoding = "Content-Encoding"

	// EmptyCompressorAlgorithm just using to beautify codes.
	EmptyCompressorAlgorithm = ""
)

// WithHeadersMap adds the all header k-v pairs into request.
func WithHeadersMap(headers map[string]string) HTTPRequestOptions {
	return func(req *http.Request) error {
		if len(headers) > 0 {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
		return nil
	}
}

// setAcceptEncodingHeader sets the new header with specified
// algorithm.
func setAcceptEncodingHeader(r *http.Request, algo string) {
	// set Accept-Encoding header to request
	if algo == "flate" {
		// replace flate to deflate, but is really need to do this?
		algo = "deflate"
	}
	r.Header.Set(acceptEncoding, algo)
	// set header Content-Encoding and key is specified algo.
	// see: https://github.com/lf-edge/ekuiper/pull/2779#issuecomment-2071751663
	r.Header.Set(contentEncoding, algo)
}

// WithBody specifies the request body.
//
// NOTICE: If the body type is form, this function will try to convert body as map[string]any, and if retErrOnConvertFailed
// is true, it returns an error on convert failure, it converts data to a raw string if set to false.
//
// If compressor is not nil and compressAlgorithm is not empty will compress body with given message.Compressor and
// set request header with key 'Accept-Encoding' and value to given compressAlgorithm.
func WithBody(body any, bodyType string, retErrOnConvertFailed bool, compressAlgorithm string) HTTPRequestOptions {
	return func(req *http.Request) error {
		switch bodyType {
		case "none":
			setAcceptEncodingHeader(req, compressAlgorithm)
			return nil
		case "json", "text", "javascript", "html", "xml":
			var bodyReader io.Reader
			switch t := body.(type) {
			case []byte:
				bodyReader = bytes.NewBuffer(t)
			case string:
				bodyReader = strings.NewReader(t)
			default:
				vj, err := json.Marshal(body)
				if err != nil {
					return fmt.Errorf("invalid content: %v", body)
				}
				body = bytes.NewBuffer(vj)
			}

			rc, ok := bodyReader.(io.ReadCloser)
			if !ok && bodyReader != nil {
				rc = io.NopCloser(bodyReader)
			}
			req.Body = rc
			// set content type with body type
			req.Header.Set("Content-Type", BodyTypeMap[bodyType])
		case "form":
			form := url.Values{}
			im, err := convertToMap(body, retErrOnConvertFailed)
			if err != nil {
				return err
			}

			for key, value := range im {
				var vstr string
				switch value.(type) {
				case []interface{}, map[string]interface{}:
					if temp, err := json.Marshal(value); err != nil {
						return fmt.Errorf("fail to parse from value: %v", err)
					} else {
						vstr = string(temp)
					}
				default:
					vstr = fmt.Sprintf("%v", value)
				}
				form.Set(key, vstr)
			}

			encodedFormBody := form.Encode()
			req.Body = io.NopCloser(strings.NewReader(encodedFormBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
		default:
			return fmt.Errorf("unsupported body type %s", bodyType)
		}

		return nil
	}
}
