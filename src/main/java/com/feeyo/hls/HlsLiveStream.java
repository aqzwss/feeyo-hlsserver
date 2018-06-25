package com.feeyo.hls;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.audio.codec.Decoder;
import com.feeyo.audio.codec.PcmuDecoder;
import com.feeyo.audio.volume.VolumeControl;
import com.feeyo.hls.ads.AdsMagr;
import com.feeyo.hls.ts.TsSegment;
import com.feeyo.hls.ts.segmenter.AacH264MixedTsSegmenter;
import com.feeyo.hls.ts.segmenter.AacTranscodingTsSegmenter;
import com.feeyo.hls.ts.segmenter.AacTsSegmenter;
import com.feeyo.hls.ts.segmenter.AbstractTsSegmenter;
import com.feeyo.hls.ts.segmenter.H264TranscodingTsSegmenter;
import com.feeyo.hls.ts.segmenter.H264TsSegmenter;
import com.feeyo.net.udp.packet.V5PacketType;
import com.google.common.collect.ImmutableSet;


/**
 * HLS 在线流
 * 
 * cache the media for N ts segment, waiting for the client to access
 * 
 * @author tribf wangyamin@variflight.com
 * @author xuwenfeng
 * @author zhuam
 */
public class HlsLiveStream {
	
	private static Logger LOGGER = LoggerFactory.getLogger( HlsLiveStream.class );
	
	private static final int SESSION_TIMEOUT_MS = 1000 * 60 * 1;			
	private static final int TS_TIMEOUT_MS = SESSION_TIMEOUT_MS * 5;			
	
	// pcmu decode
	private static Decoder pcmuDecoder = new PcmuDecoder();
    
    // id -> client session
    private Map<String, HlsClientSession> clientSessions = new ConcurrentHashMap<String, HlsClientSession>();
    
    private long ctime;
    private long mtime;
    
    private long streamId;
    private int streamType;
    
    // audio
    private float sampleRate;
    private int sampleSizeInBits;
    private int channels;
    
    // video
    private int fps;
    
    // asias
    private List<String> aliasNames;
    
    private long rawCount = 0;

    //
    private AbstractTsSegmenter tsSegmenter = null;
    
    //
    private Map<Long, TsSegment> tsSegments = new ConcurrentHashMap<Long, TsSegment>(); 		
    private AtomicLong tsIndexGen = new AtomicLong(4);										//  ads 1,2,3   normal 4...  
    
    private VolumeControl volumeCtl = null;
    private volatile boolean isNoiseReduction = false;
    private volatile boolean isNoiseCompensate = true;

    public HlsLiveStream(Long streamId, Integer streamType, List<String> aliasNames, 
    		Float sampleRate, Integer sampleSizeInBits, Integer channels, Integer fps) {
        
    	this.ctime = System.currentTimeMillis();
    	this.mtime = ctime;
    	
    	this.streamId = streamId;
    	this.streamType = streamType;
    	
    	this.aliasNames = aliasNames;
    	
        this.sampleRate = sampleRate == null ? 8000F : sampleRate;
        this.sampleSizeInBits = sampleSizeInBits == null ? 16: sampleSizeInBits;
        this.channels = channels == null ? 1 : channels;
        this.fps = fps == null ? 25: fps;
        
        
        switch( streamType ) {
    	case HlsLiveStreamType.PCM:
    		tsSegmenter = new AacTranscodingTsSegmenter();
    		break;
    	case HlsLiveStreamType.AAC:
    		tsSegmenter = new AacTsSegmenter();
    		break;
    	case HlsLiveStreamType.YUV:
    		tsSegmenter = new H264TranscodingTsSegmenter();
    		break;
    	case HlsLiveStreamType.H264:
    		tsSegmenter = new H264TsSegmenter();
    		break;
    	case HlsLiveStreamType.AAC_H264_MIXED:
    		tsSegmenter = new AacH264MixedTsSegmenter();
    		break;
    	}
        
        tsSegmenter.initialize(sampleRate, sampleSizeInBits, channels, fps);
    }
    
    
    //
    public void removeExpireSessionAndTsSegments() {
    	
    	long now = System.currentTimeMillis();
    	
    	// remove expire SESSION
		for (HlsClientSession session : clientSessions.values()) {
			// remove expire session
			if (now - session.getMtime() > SESSION_TIMEOUT_MS) {
				clientSessions.remove( session.getId() );
				LOGGER.info("##streamId={},  remove session sid:{},  cache size={} ", streamId, session.getId(), clientSessions.size());
			}
		}
  
		// remove expire TS SEGMENT 
		for(Map.Entry<Long, TsSegment> entry:  tsSegments.entrySet() ) {
			long tsIndex =  entry.getKey();
			TsSegment tsSegment = entry.getValue();
			
			if ( ((now - tsSegment.getCtime()) > TS_TIMEOUT_MS) && tsSegments.size() > 5 ) {
				tsSegments.remove( tsIndex );
				LOGGER.info("##streamId={},  remove ts index={},  cache size={} ", streamId, tsIndex, tsSegments.size());
			} 
		}
    }
    
    // length= 3 ~ 5
    public long[] fetchTsIndexs() {
    	// 
    	Set<Long> indexSET = ImmutableSet.copyOf(tsSegments.keySet());
    	if ( indexSET.size() < 3 ) {
    		return null;
    	}	
    	
    	//
    	Long[] indexArr = indexSET.toArray(new Long[indexSET.size()]);
    	Arrays.sort( indexArr );
    	
    	if ( indexArr.length > 5 ) {
    		Long[] tmpArr = new Long[5];
    		System.arraycopy(indexArr, indexArr.length - 5, tmpArr, 0, 5);
    		
    		return ArrayUtils.toPrimitive( tmpArr );
    		
    	} else {
    		return ArrayUtils.toPrimitive( indexArr );
    	}
    }
    
    public TsSegment fetchTsSegmentByIndex(long index) {
    	
    	if ( index < 0 )
    		return null;
    	
    	TsSegment tsSegment = null;
    	if ( index < 4 ) {
    		
    		String type = "audio";
    		switch( streamType ) {
        	case HlsLiveStreamType.YUV:
        	case HlsLiveStreamType.H264:
        		type = "video";
        		break;
        	case HlsLiveStreamType.AAC_H264_MIXED:
        		type = "mixed";
        		break;
        	}
    		
    		List<TsSegment> adTsSegments = AdsMagr.getTsSegments(type, sampleRate, sampleSizeInBits, channels, fps);
    		tsSegment = adTsSegments.get((int)index - 1);
    		
    	} else {
    		tsSegment = tsSegments.get( index );
    	}
		
    	if ( tsSegment != null ) {
    		tsSegment.setLasttime(  System.currentTimeMillis() );
    	}
    	
    	return tsSegment;
	}
    
    //
    public HlsClientSession newClientSession() {
    	
    	// 避免在TS未准备好的情况下， client 数膨胀
    	if ( tsSegments.size() < 3 ) {
    		return null;
    	}
        
        HlsClientSession clientSession = new HlsClientSession(this);
        clientSessions.put(clientSession.getId(), clientSession);
        
        LOGGER.info("##streamId={},  add client:{} ", streamId, clientSession);
        return clientSession;
    }
    
    public HlsClientSession getClientSessionsById(String id) {
    	HlsClientSession clientSession = clientSessions.get(id);
    	return clientSession;
    }
    
    public Map<String, HlsClientSession> getAllClientSession() {
        return clientSessions;
    }
    
    public void close() {
    	if ( clientSessions != null)
    		clientSessions.clear();
        
        if ( tsSegmenter != null)
        	tsSegmenter.close();
        
        if ( tsSegments != null ) {
        	tsSegments.clear();
        }
    }
    
    public long getRawCount() {
		return rawCount;
	}

	public long getMtime() {
        return mtime;
    }

    public long getCtime() {
		return ctime;
	}

	public long getStreamId() {
        return streamId;
    }
    
    public int getStreamType() {
		return streamType;
	}


	// 采样率
    public float getSampleRate() {
		return sampleRate;
	}

	public int getSampleSizeInBits() {
		return sampleSizeInBits;
	}

	public int getChannels() {
		return channels;
	}

	public int getFps() {
		return fps;
	}

	public List<String> getAliasNames() {
		return aliasNames;
	}

	public void setAliasNames(List<String> aliasNames) {
		this.aliasNames = aliasNames;
	}

	// 是否降噪处理
	// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	public boolean getIsNoiseReduction() {
		return isNoiseReduction;
	}

	public void setNoiseReduction(boolean isNoiseReduction) {
		this.isNoiseReduction = isNoiseReduction;
	}
	
	// 是否产生白噪音进行噪音弥补
	public boolean getIsNoiseCompensate() {
		return isNoiseCompensate;
	}
	
	public void setNoiseCompensate(boolean isNoiseCompensate) {
		this.isNoiseCompensate = isNoiseCompensate;
	}

	//
	public long getLastIndex() {
		return this.tsIndexGen.get();
	}
	
	//
	// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	public synchronized void addAvStream(byte rawType, byte[] rawData, byte[] rawReserved, int frameLength) {
    	
    	long now = System.currentTimeMillis();
    	
    	this.mtime = now;
    	this.rawCount++;

    	if( tsSegmenter != null) {
    		
    		// PCM transcode & Audio noise reduce / vol gain
    		if (V5PacketType.PCM_STREAM == rawType) {
    			rawData = pcmuDecoder.process( rawData );
				
				if( volumeCtl == null ) {
					volumeCtl = new VolumeControl((int)sampleRate, frameLength);
				}

				// 增益
				rawData = volumeCtl.gain( rawData );
				
				// 降噪
				if ( isNoiseReduction ) {
					rawData = volumeCtl.noise( rawData );
				}
				
				// 注入白噪声
				if( isNoiseCompensate ) {
					rawData = volumeCtl.silenceDetection( rawData );
				}
			}
    		
	        byte[] tsData = tsSegmenter.getTsBuf( rawType, rawData, rawReserved );
	        if ( tsData != null) {
	        	
	        	long tsIndex = tsIndexGen.getAndIncrement();
	            TsSegment tsSegment = new TsSegment(  tsIndex +".ts", tsData, tsSegmenter.getTsSegTime(), false);
	            tsSegments.put(tsIndex, tsSegment);
	            
	            LOGGER.info("##streamId={},  add ts {}", streamId, tsSegment);
	        }
    	}
    }
	
}
