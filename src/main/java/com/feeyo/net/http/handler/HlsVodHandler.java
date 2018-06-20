package com.feeyo.net.http.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.model.ObjectMetadata;
import com.feeyo.audio.codec.faac.FaacUtils;
import com.feeyo.hls.HlsLiveStream;
import com.feeyo.hls.HlsLiveStreamMagr;
import com.feeyo.hls.ts.segmenter.AacTranscodingTsSegmenter;
import com.feeyo.net.http.util.HttpUtil;
import com.feeyo.net.http.util.OssUtil;
import com.feeyo.net.udp.packet.V5PacketType;
import com.feeyo.util.DefaultThreadFactory;

/**
 *  vod stream request handler
 *  
 * @author tribf wangyamin@variflight.com
 *
 */
public class HlsVodHandler implements IRequestHandler {
	
	private final Logger LOGGER = LoggerFactory.getLogger(HlsVodHandler.class);
	
	
	private static final String regex = "^/hls/vod/\\w+/\\w+\\.(m3u8|ts)$";
    private static final int VOD_CACHE_TIME = 1000 * 60;

    private ThreadPoolExecutor executor =  new ThreadPoolExecutor(4, 10, 6 * 1000L, 
    		TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(500), new DefaultThreadFactory("HlsVod-", true));
    
    private static Map<String, byte[]> cachedVodTsFiles = new ConcurrentHashMap<String, byte[]>();
    private static final HashSet<String> m3u8WaiteSet = new HashSet<>();

    @Override
    public Type getType() {
        return IRequestHandler.Type.HLS;
    }

    @Override
    public boolean isFilted() {
        return true;
    }

    @Override
    public void execute(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    	
        HttpRequest request = (DefaultHttpRequest) e.getMessage();
        if ( !request.getUri().matches(regex) ) {
            LOGGER.warn("bad request: " + request.getUri());
            HttpUtil.sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
        }

        
        String[] path = request.getUri().split("/");
        String alias = path[3];
        final String reqFileName = path[4];
        
        Long streamId = HlsLiveStreamMagr.INSTANCE().getStreamIdByAlias(alias);
        if( streamId == null ) {
            LOGGER.warn(" aac vod, lookup alias failed: " + alias);
            HttpUtil.sendError(ctx, HttpResponseStatus.NOT_FOUND);
            return;
        }
        
        
        OssUtil ossOperation = new OssUtil();
        byte[] content = null;

        if (reqFileName.endsWith(".m3u8")) {
        	
            if ( !ossOperation.doesObjectExist(reqFileName, streamId) ) {
            	
                boolean needWaite = false;
                // the very first listener of a specific m3u8 will create the m3u8 file and the ts file
                // the reset will be in a waiter set
                synchronized (m3u8WaiteSet) {
                    if (!m3u8WaiteSet.add(reqFileName)) {
                        needWaite = true;
                    }
                }

                if (needWaite) {
                    synchronized (m3u8WaiteSet) {
                        while (m3u8WaiteSet.contains(reqFileName)) {
                            m3u8WaiteSet.wait(1000);
                        }
                    }
                } else {
                    content = generateTsFiles(reqFileName, streamId);
                }
            }
        }

        if (content == null) {
            content = cachedVodTsFiles.get(reqFileName);
            if (content == null) {
                if (ossOperation.doesObjectExist(reqFileName, streamId)){
                    InputStream inputStream = ossOperation.readObject(reqFileName, streamId);

                    ObjectMetadata objectMetadata = ossOperation.getObjectMetadata(reqFileName,streamId);
                    int len = (int)objectMetadata.getContentLength();
                    content = new byte[len];

                    int writePtr = 0;
                    for (;;) {
                        int ret = inputStream.read(content, writePtr,len-writePtr);
                        if ((ret == -1) || (writePtr += ret) >= len)
                            break;
                    }
                } else {
                    LOGGER.warn("request file not on OSS: " + request.getUri());
                    HttpUtil.sendError(ctx, HttpResponseStatus.NOT_FOUND);
                    return;
                }
            }
        }

        ossOperation.closeOSSClient();

        long timeMillis = System.currentTimeMillis();

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaders.Names.DATE, HttpUtil.getDateString(timeMillis));
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, HttpUtil.getMimeType(reqFileName));
        response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, content.length);
        response.headers().set(HttpHeaders.Names.LAST_MODIFIED, HttpUtil.getDateString(timeMillis));
        response.headers().set(HttpHeaders.Names.EXPIRES, HttpUtil.getDateString(timeMillis + VOD_CACHE_TIME));
        response.headers().set(HttpHeaders.Names.CACHE_CONTROL, "max-age=" + (VOD_CACHE_TIME/1000));

        response.setContent(ChannelBuffers.copiedBuffer(content));

        e.getChannel().write(response);
    }

    
    //
    final Object _lock = new Object();
    
    private synchronized byte[] generateTsFiles(final String reqFileName, final long streamId) throws InterruptedException {
        
    	final String wavFileName = reqFileName.replace(".m3u8",".wav");
        
        final OssUtil ossOperation = new OssUtil();
        ObjectMetadata objectMetadata = ossOperation.getObjectMetadata(wavFileName, streamId);
        
        final AacTranscodingTsSegmenter tsSegmenter = new AacTranscodingTsSegmenter();
        final int tsNum = tsSegmenter.calcTsNum((int)objectMetadata.getContentLength());
        
        HlsLiveStream stream = HlsLiveStreamMagr.INSTANCE().getHlsLiveStreamById(streamId);
        float sampleRate = stream != null ? stream.getSampleRate() : 8000F;

        // 提取OSS 语音文件，生成TS文件， 然后再上传至 OSS
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        StringBuilder m3u8 = new StringBuilder();
        m3u8.append("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:"+ AacTranscodingTsSegmenter.TS_DURATION +"\n#EXT-X-MEDIA-SEQUENCE:"+ 1);
		for (int i = 0; i < tsNum; i++) {
			m3u8.append("\n#EXTINF:" + tsSegmenter.calcTsSegTime(sampleRate) + ",\n" + reqFileName.replace(".m3u8", "") + "_" + i + ".ts");
		}
        m3u8.append("\n#EXT-X-ENDLIST");
        ossOperation.uploadObject(m3u8.toString().getBytes(), reqFileName, streamId);

        executor.execute(new Runnable() {
        	
        	private int readBytes(InputStream inputStream, byte[] outputBuf) throws IOException {
                int length = outputBuf.length;
                int frameBufPtr = 0;
                int len;

                while(frameBufPtr < length) {
                    len = inputStream.read(outputBuf,frameBufPtr,length-frameBufPtr);
                    if (len == -1) {
                        if (frameBufPtr == 0) {
                            return -1;
                        }
                        Arrays.fill(outputBuf, frameBufPtr, length, (byte) 0x0);
                        break;
                    }
                    frameBufPtr += len;
                }
                return frameBufPtr;
            }
        	
            @Override
            public void run() {
            	
                List<String> cachedTsName = new ArrayList<>();
                byte[] frameBuf = new byte[2048];
                byte[] tsSegment;

                InputStream inputStream = ossOperation.readObject(wavFileName, streamId);
                try {
                	
                    long beginTime = System.currentTimeMillis();
                    
					for (int i = 0; i < tsNum;) {
						
						if ( readBytes(inputStream, frameBuf) > 0 ) {
							tsSegment = tsSegmenter.getTsBuf(V5PacketType.PCM_STREAM, frameBuf, null);

						} else {
							tsSegment = tsSegmenter.getTsBuf(V5PacketType.PCM_STREAM, FaacUtils.ZERO_PCM_DATA, null);
						}

                        if (tsSegment != null) {
                        	
                        	StringBuffer tsNameSb = new StringBuffer();
                        	tsNameSb.append( reqFileName.replace(".m3u8", "") ).append("_").append( i ).append(".ts");
							String tsName = tsNameSb.toString();
                            
                            // 初始化，缓存5个 tsSegment
                            if (i < 5)  {
                                cachedTsName.add(tsName);
                                cachedVodTsFiles.put(tsName, tsSegment);
								if ( i == 4 ) {
                                    // 缓存完成后，通知m3u8请求线程
                                    synchronized (_lock) { 
                                    	_lock.notifyAll(); 
                                    }
                                }
								
                            } else {
                                ossOperation.uploadObject(tsSegment, tsName, streamId);
                            }
                            i++;
                        }
                    }

                    // in case too short the vod file is
                    synchronized (_lock) { 
                    	_lock.notifyAll(); 
                    }

                    // upload the cached file and then remove it
                    for (String name : cachedTsName) {
                        byte[] ts = cachedVodTsFiles.get(name);
                        ossOperation.uploadObject(ts, name, streamId);
                        cachedVodTsFiles.remove(name);
                    }

                    long endTime = System.currentTimeMillis();
                    LOGGER.debug("VOD on " + reqFileName + ", tsNum: " + tsNum + " cost: " + ((endTime-beginTime)/1000.0) +" s");

                } catch (IOException e1) {
                    LOGGER.error("faile when generate ts files",e1);
                    // when error happens, delete m3u8 and next time re create the ts files
                    ossOperation.deleteObject(reqFileName, streamId);
                } finally {
                    ossOperation.closeOSSClient();
                    tsSegmenter.close();
                }
            }
        });

        synchronized (_lock) {
            _lock.wait(1000);
        }

        // rmove current m3u8 from the waite set
        synchronized (m3u8WaiteSet) {
            m3u8WaiteSet.remove(reqFileName);
            m3u8WaiteSet.notifyAll();
        }

        return m3u8.toString().getBytes();
    }
}
