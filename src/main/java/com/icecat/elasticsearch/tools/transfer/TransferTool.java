package com.icecat.elasticsearch.tools.transfer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

public class TransferTool {

    public static void main(String[] args) throws ClientProtocolException, IOException {
        Options options = new Options();
        Option settingOp = new Option("setting", false, "迁移settings");
        Option mappingOp = new Option("mapping", false, "迁移mappings");
        Option dataOp = new Option("data", false, "迁移数据");
        Option sourceHostOp = new Option("sh", "source-host", true, "数据源");
        Option sourceIndexOp = new Option("si", "source-index", true, "要迁移的index");
        Option sourceTypeOp = new Option("st", "source-type", true, "要迁移的type");
        Option targetHostOp = new Option("th", "target-host", true, "数据源");
        Option targetIndexOp = new Option("ti", "target-index", true, "迁移后的index");
        Option targetTypeOp = new Option("tt", "target-type", true, "迁移后的type");
        options.addOption(settingOp).addOption(mappingOp).addOption(dataOp).addOption(sourceHostOp).addOption(sourceIndexOp).addOption(sourceTypeOp).addOption(targetHostOp).addOption(targetIndexOp).addOption(targetTypeOp);
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (!line.hasOption("sh") || !line.hasOption("th")) {
                formatter.printHelp("transfer", options);
                return;
            }
            if (line.hasOption("setting") && line.hasOption("si") && line.hasOption("ti")) {
                transferSetting(line.getOptionValue("sh"), line.getOptionValue("si"), line.getOptionValue("th"), line.getOptionValue("ti"));
            } else if (line.hasOption("mapping") && line.hasOption("si") && line.hasOption("ti")) {
                if (!line.hasOption("st") && !line.hasOption("tt")) {
                    transferMapping(line.getOptionValue("sh"), line.getOptionValue("si"), line.getOptionValue("th"), line.getOptionValue("ti"));
                } else if (line.hasOption("st") && line.hasOption("tt")) {
                    transferMapping(line.getOptionValue("sh"), line.getOptionValue("si"), line.getOptionValue("st"), line.getOptionValue("th"), line.getOptionValue("ti"), line.getOptionValue("tt"));
                } else {
                    System.err.println("Parsing failed.  Reason: Missing type of source or target");
                }
            } else if (line.hasOption("data")) {
                if (!line.hasOption("st") && !line.hasOption("tt")) {
                    transferData(line.getOptionValue("sh"), line.getOptionValue("si"), line.getOptionValue("th"), line.getOptionValue("ti"));
                } else if (line.hasOption("st") && line.hasOption("tt")) {
                    transferData(line.getOptionValue("sh"), line.getOptionValue("si"), line.getOptionValue("st"), line.getOptionValue("th"), line.getOptionValue("ti"), line.getOptionValue("tt"));
                } else {
                    System.err.println("Parsing failed.  Reason: Missing type of source or target");
                }
            } else {
                formatter.printHelp("transfer", options);
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }
    }

    public static void transferSetting(final String sourceHost, final String sourceIndex, final String targetHost, final String targetIndex) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpget = new HttpGet("http://" + sourceHost + "/" + sourceIndex + "/_settings");
            System.out.println("Executing request " + httpget.getRequestLine());
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                @Override
                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        return EntityUtils.toString(response.getEntity(), "UTF-8");
                    }
                    throw new ClientProtocolException("Unexpected response status: " + status + ", " + response.getStatusLine().getReasonPhrase());
                }

            };
            String settings = httpclient.execute(httpget, responseHandler);
            HttpPost httpPost = new HttpPost("http://" + targetHost + "/" + targetIndex);
            System.out.println("Executing request " + httpPost.getRequestLine());
            StringEntity entity = new StringEntity(settings, "UTF-8");
            httpPost.setEntity(entity);
            CloseableHttpResponse createIndexResponse = httpclient.execute(httpPost);
            int status = createIndexResponse.getStatusLine().getStatusCode();
            if (status < 200 || status >= 300) {
                throw new ClientProtocolException("Unexpected response status: " + status + ", " + createIndexResponse.getStatusLine().getReasonPhrase());
            }
        } finally {
            httpclient.close();
        }
    }

    public static void transferMapping(final String sourceHost, final String sourceIndex, final String targetHost, final String targetIndex) throws ClientProtocolException, IOException {
        List<String> types = selectAllTypes(sourceHost, sourceIndex);
        for (String type : types) {
            try {
                transferMapping(sourceHost, sourceIndex, type, targetHost, targetIndex, type);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void transferMapping(final String sourceHost, final String sourceIndex, final String sourceType, final String targetHost, final String targetIndex, String targetType) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpget = new HttpGet("http://" + sourceHost + "/" + sourceIndex + "/_mapping/" + sourceType);
            System.out.println("Executing request " + httpget.getRequestLine());
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                @Override
                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    final Gson gson = new Gson();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        Map map = gson.fromJson(EntityUtils.toString(entity, "UTF-8"), Map.class);
                        Map typeMapping = (Map) (((Map) (map.get(sourceIndex))).get("mappings"));
                        return gson.toJson(typeMapping);
                    }
                    throw new ClientProtocolException("Unexpected response status: " + status + ", " + response.getStatusLine().getReasonPhrase());
                }

            };
            String typeMapping = httpclient.execute(httpget, responseHandler);
            HttpPost httpPost = new HttpPost("http://" + targetHost + "/" + targetIndex + "/_mapping/" + targetType);
            System.out.println("Executing request " + httpPost.getRequestLine());
            StringEntity entity = new StringEntity(typeMapping, "UTF-8");
            httpPost.setEntity(entity);
            CloseableHttpResponse createIndexResponse = httpclient.execute(httpPost);
            int status = createIndexResponse.getStatusLine().getStatusCode();
            if (status < 200 || status >= 300) {
                throw new ClientProtocolException("Unexpected response status: " + status + ", " + createIndexResponse.getStatusLine().getReasonPhrase());
            }
        } finally {
            httpclient.close();
        }
    }

    public static void transferData(final String sourceHost, final String sourceIndex, final String targetHost, final String targetIndex) throws ClientProtocolException, IOException {
        List<String> types = selectAllTypes(sourceHost, sourceIndex);
        for (String type : types) {
            try {
                transferData(sourceHost, sourceIndex, type, targetHost, targetIndex, type);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void transferData(final String sourceHost, final String sourceIndex, final String sourceType, final String targetHost, final String targetIndex, final String targetType) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {

            HttpPost httppost = new HttpPost("http://" + sourceHost + "/" + sourceIndex + "/" + sourceType + "/_search");
            final Gson gson = new Gson();
            Map<String, Object> map = Maps.newHashMap();
            int size = 1000;
            double count = docsCount(sourceHost, sourceIndex, sourceType);
            for (int from = 0; from < count; from += size) {
                map.put("from", from);
                map.put("size", size);
                StringEntity myEntity = new StringEntity(gson.toJson(map), "UTF-8");
                httppost.setEntity(myEntity);
                System.out.println("Executing request " + httppost.getRequestLine());
                ResponseHandler<List<Map>> responseHandler = new ResponseHandler<List<Map>>() {

                    @Override
                    public List<Map> handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                        List<Map> list = Lists.newArrayList();
                        int status = response.getStatusLine().getStatusCode();
                        if (status >= 200 && status < 300) {
                            HttpEntity entity = response.getEntity();
                            Map map = gson.fromJson(EntityUtils.toString(entity, "UTF-8"), Map.class);
                            List<Map> hits = (List) (((Map) (map.get("hits"))).get("hits"));
                            for (Map hitMap : hits) {
                                list.add(((Map) hitMap.get("_source")));
                            }
                        } else {
                            throw new ClientProtocolException("Unexpected response status: " + status);
                        }
                        return list;
                    }

                };
                createDocs(targetHost, targetIndex, targetType, httpclient.execute(httppost, responseHandler));
            }
        } finally {
            httpclient.close();
        }
    }

    private static double docsCount(final String host, final String index, final String type) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet httpget = new HttpGet("http://" + host + "/" + index + "/" + type + "/_count");
            System.out.println("Executing request " + httpget.getRequestLine());
            ResponseHandler<Double> responseHandler = new ResponseHandler<Double>() {

                @Override
                public Double handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    final Gson gson = new Gson();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        Map map = gson.fromJson(EntityUtils.toString(entity, "UTF-8"), Map.class);
                        return Double.parseDouble(map.get("count").toString());
                    }
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }

            };
            return httpclient.execute(httpget, responseHandler);
        } finally {
            httpclient.close();
        }
    }

    private static void createDocs(final String host, final String index, final String type, final List<Map> docs) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        StringBuilder messages = new StringBuilder();
        Gson gson = new Gson();
        for (Map doc : docs) {
            messages.append("{ \"create\" : { \"_index\" : \"" + index + "\", \"_type\" : \"" + type + "\" } }").append("\n");
            messages.append(gson.toJson(doc)).append("\n");
        }
        try {
            HttpPost httpget = new HttpPost("http://" + host + "/_bulk");
            StringEntity entity = new StringEntity(messages.toString(), "UTF-8");
            httpget.setEntity(entity);
            System.out.println("Executing request " + httpget.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httpget);
            if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() >= 300) {
                throw new ClientProtocolException("Unexpected response status: " + response.getStatusLine().getStatusCode());
            }
        } finally {
            httpclient.close();
        }
    }

    private static List<String> selectAllTypes(final String host, final String index) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            final Gson gson = new Gson();
            HttpGet httpget = new HttpGet("http://" + host + "/" + index + "/_mapping/");
            System.out.println("Executing request " + httpget.getRequestLine());
            ResponseHandler<List<String>> responseHandler = new ResponseHandler<List<String>>() {

                @Override
                public List<String> handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        Map map = gson.fromJson(EntityUtils.toString(entity, "UTF-8"), Map.class);
                        Map mappings = (Map) (((Map) (map.get(index))).get("mappings"));
                        return Lists.newArrayList(Iterators.transform(mappings.keySet().iterator(), new Function<Object, String>() {

                            @Override
                            public String apply(Object type) {
                                return type.toString();
                            }

                        }));
                    }
                    throw new ClientProtocolException("Unexpected response status: " + status + ", " + response.getStatusLine().getReasonPhrase());
                }

            };
            return httpclient.execute(httpget, responseHandler);
        } finally {
            httpclient.close();
        }
    }
}
