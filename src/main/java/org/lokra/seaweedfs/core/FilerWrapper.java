package org.lokra.seaweedfs.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.util.CharsetUtils;
import org.lokra.seaweedfs.core.http.JsonResponse;
import org.lokra.seaweedfs.exception.SeaweedfsException;
import org.lokra.seaweedfs.exception.SeaweedfsFileNotFoundException;

/**
 *
 * @author David Ricardo
 */
public class FilerWrapper
{
  private Connection connection;
  private ObjectMapper objectMapper = new ObjectMapper();
  
  FilerWrapper(Connection Connection) {
        this.connection = Connection;
    }
  
  JsonResponse uploadFile(String urlstring, String fileName, InputStream stream, ContentType contentType) throws IOException
  {
    HttpPost request;
    URL url = new URL(urlstring + "/" + fileName);
    request = new HttpPost(url.toString());

    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE).setCharset(CharsetUtils.get("UTF-8"));
    builder.addBinaryBody("upload", stream, contentType, fileName);
    HttpEntity entity = builder.build();
    request.setEntity(entity);
    
    JsonResponse jsonResponse = connection.fetchJsonResultByRequest(request);
    convertResponseStatusToException(jsonResponse.statusCode, url.toString(), fileName, false, false, false, false);
    
    //return (Integer) objectMapper.readValue(jsonResponse.json, Map.class).get("size");
    return jsonResponse;
  }
  
  private void convertResponseStatusToException(int statusCode, String url, String filename,
                                                  boolean ignoreNotFound,
                                                  boolean ignoreRedirect,
                                                  boolean ignoreRequestError,
                                                  boolean ignoreServerError) throws SeaweedfsException {

        switch (statusCode / 100) {
            case 1:
                return;
            case 2:
                return;
            case 3:
                if (ignoreRedirect)
                    return;
                throw new SeaweedfsException(
                        "fetch file from [" + url + "/" + filename + "] is redirect, " +
                                "response stats code is [" + statusCode + "]");
            case 4:
                if (statusCode == 404 && ignoreNotFound)
                    return;
                else if (statusCode == 404)
                    throw new SeaweedfsFileNotFoundException(
                            "fetch file from [" + url + "/" + filename + "] is not found, " +
                                    "response stats code is [" + statusCode + "]");
                if (ignoreRequestError)
                    return;
                throw new SeaweedfsException(
                        "fetch file from [" + url + "/" + filename + "] is request error, " +
                                "response stats code is [" + statusCode + "]");
            case 5:
                if (ignoreServerError)
                    return;
                throw new SeaweedfsException(
                        "fetch file from [" + url + "/" + filename + "] is request error, " +
                                "response stats code is [" + statusCode + "]");
            default:
                throw new SeaweedfsException(
                        "fetch file from [" + url + "/" + filename + "] is error, " +
                                "response stats code is [" + statusCode + "]");
        }
    }
}
