package com.feeyo.hls;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.udp.packet.V5Packet;
import com.feeyo.net.udp.packet.V5PacketType;
import com.feeyo.util.DefaultThreadFactory;

/**
 * HLS LIVE Stream MANAGE
 *
 */
public class HlsLiveStreamMagr {
	
	private static final Logger LOGGER = LoggerFactory.getLogger( HlsLiveStreamMagr.class );
	
    private static volatile HlsLiveStreamMagr instance;
    
	private static final int LIVE_STREAM_TIMEOUT_MS = 1000 * 60 * 10;		// ten minute
	
	// dispatch thread pool
    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 100, 60 * 1000L, 
    		TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000), new DefaultThreadFactory("HlsLsMagr-", true));
    
    private static ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newScheduledThreadPool(3);

    // alias -> streamId 
	private Map<String, Long> aliasToStreamIdCache = new ConcurrentHashMap<String, Long>();
    
    // streamId -> live stream
    private Map<Long, HlsLiveStream> streamIdToLiveStreamCache = new ConcurrentHashMap<Long, HlsLiveStream>();
    
    
    private HlsLiveStreamMagr() {}
    
    private static Object _lock = new Object();
    
    public static HlsLiveStreamMagr INSTANCE() {
    	if(instance == null ) {
    		synchronized (_lock) {
				if(instance == null)
					instance = new HlsLiveStreamMagr();
    		}
    	}
    	
        return instance;
    }
    
    
    public void startup() {
    	
		// delete expired liveStream & clientSession
    	scheduledThreadPoolExecutor.scheduleAtFixedRate( new Runnable() {

			@Override
			public void run() {
				
				// expired
				try {
	            	
					long now = System.currentTimeMillis();
	            	
	            	Iterator<HlsLiveStream> it = streamIdToLiveStreamCache.values().iterator();
	            	while( it.hasNext() ) {
	            		
	            		HlsLiveStream liveStream =  it.next();
	            		if (now - liveStream.getMtime() > LIVE_STREAM_TIMEOUT_MS) {
		                       
	                        //
	                        long streamId = liveStream.getStreamId();
	                        streamIdToLiveStreamCache.remove(streamId);
	                        Iterator<Long> iter = aliasToStreamIdCache.values().iterator();
	                        while(iter.hasNext()) {
	                        	if(streamId == iter.next())
	                        		iter.remove();
	                        }
	                        
	                        //
	                        liveStream.close();
	
	                    } else {
	                    	// 
	                    	liveStream.removeExpireSessionAndTsSegments();
	                    }
	            		
	            	}
	                
				} catch(Throwable e) {
					LOGGER.warn("live stream task err:", e);
				}
              
			}
    		
    	}, 10, 10, TimeUnit.SECONDS);
    
    }

    public void close() {
    	
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdown();
        }
        
        if ( scheduledThreadPoolExecutor != null ) {
        	scheduledThreadPoolExecutor.shutdown();
        }
    }
    
    
    public HlsLiveStream getHlsLiveStreamById(Long id) {
    	return streamIdToLiveStreamCache.get( id );
    }
    
    public HlsLiveStream getHlsLiveStreamByAlias(String alias) {
    	Long streamId = aliasToStreamIdCache.get(alias);
    	if ( streamId == null ) 
    		return null;
    	return streamIdToLiveStreamCache.get( streamId );
    }
    
    public Long getStreamIdByAlias(String alias) {
    	return aliasToStreamIdCache.get(alias);
    }
    
    public Collection<HlsLiveStream> getAllLiveStream(){
    	return streamIdToLiveStreamCache.values();
    }
    
    
    
    public boolean updateHlsLiveStreamNoiseReductionById(long streamId, boolean isNoiseReduction) {
    	
    	HlsLiveStream liveStream = streamIdToLiveStreamCache.get( streamId );
    	if ( liveStream != null ) {
    		liveStream.setNoiseReduction(isNoiseReduction);
    		return true;
    	}
    	return false;
    }
    
    public boolean updateHlsLiveStreamNoiseCompensateById(long streamId, boolean isNoiseCompensate) {
    	
    	HlsLiveStream liveStream = streamIdToLiveStreamCache.get( streamId );
    	if ( liveStream != null ) {
    		liveStream.setNoiseCompensate(isNoiseCompensate);
    		return true;
    	}
    	return false;
    }
    
    public boolean updateHlsLiveStreamAliasNamesById(long streamId, List<String> newAliasNames) {
    	
    	HlsLiveStream liveStream = streamIdToLiveStreamCache.get( streamId );
    	if ( liveStream != null ) {
    		
    		List<String> oldAliasNames = liveStream.getAliasNames();
    		for(String oldName: oldAliasNames) {
    			aliasToStreamIdCache.remove( oldName );
    		}
    		
    		for(String newName: newAliasNames) {
    			aliasToStreamIdCache.put(newName, streamId);
    		}
    		
    		liveStream.setAliasNames( newAliasNames );
    		return true;
    	}
    	return false;
    }
    
    public void startHlsLiveStream(Long streamId, Integer streamType, List<String> aliasNames, 
    		Float sampleRate, Integer sampleSizeInBits, Integer channels, Integer fps) {
    	
    	HlsLiveStream liveStream = streamIdToLiveStreamCache.get(streamId);
		if ( liveStream == null ) {
			liveStream = new HlsLiveStream(streamId, streamType, aliasNames, sampleRate, sampleSizeInBits, channels, fps);
			streamIdToLiveStreamCache.put(streamId, liveStream);
			
			for(String aliasName : aliasNames)
				aliasToStreamIdCache.put(aliasName, streamId);
		} else {
			updateHlsLiveStreamAliasNamesById(streamId, aliasNames);
		}
		
    }
    
    public void closeHlsLiveStream(long streamId) {
    	
    	HlsLiveStream liveStream = streamIdToLiveStreamCache.remove(streamId);
		if ( liveStream != null ) {
			
			List<String> oldAliasNames = liveStream.getAliasNames();
    		for(String oldName: oldAliasNames) {
    			aliasToStreamIdCache.remove( oldName );
    		}
			
			liveStream.close();
			liveStream = null;
		}
    }
    
    public void closeAllHlsLiveStream() {
    	//
    	aliasToStreamIdCache.clear();
    	
    	Iterator<Long> iter = streamIdToLiveStreamCache.keySet().iterator();
    	while( iter.hasNext() ) {
    		Long streamId = iter.next();
    		HlsLiveStream liveStream = streamIdToLiveStreamCache.remove(streamId);
    		if ( liveStream != null ) {
    			liveStream.close();
    			liveStream = null;
    		}
    	}
    	
    }
    
	public void handleStream(final V5Packet packet) {

		
		try {		
			threadPoolExecutor.execute(new Runnable() {
	
				@Override
				public void run() {
					long packetSender = packet.getPacketSender();
					byte packetType = packet.getPacketType();
					byte[] packetData = packet.getPacketData();
					byte[] packetReserved = packet.getPacketReserved();
					int packetLength = packet.getPacketLength();
	
					HlsLiveStream liveStream = streamIdToLiveStreamCache.get(packetSender);
					if (liveStream != null) {
	
						boolean isPass = false;
						if ( liveStream.getStreamType() == HlsLiveStreamType.AAC && packetType == V5PacketType.AAC_STREAM ) {
							isPass = true;
							
						} else if ( liveStream.getStreamType() == HlsLiveStreamType.PCM && packetType == V5PacketType.PCM_STREAM ) {
							isPass = true;
	
						} else if (liveStream.getStreamType() == HlsLiveStreamType.H264 && packetType == V5PacketType.H264_STREAM ) {
							isPass = true;
							
						} else if (liveStream.getStreamType() == HlsLiveStreamType.YUV && packetType == V5PacketType.YUV422_STREAM ) {
							isPass = true;
	
						} else if (liveStream.getStreamType() == HlsLiveStreamType.AAC_H264_MIXED
								&& (packetType == V5PacketType.H264_STREAM || packetType == V5PacketType.AAC_STREAM)) {
							isPass = true;
						}
	
						if ( isPass ) {
							
							liveStream.addAvStream(packetType, packetData, packetReserved, packetLength);
							
						} else {
							LOGGER.warn("livestream no pass err: streamId={}, streamType={}, packetType={}", liveStream.getStreamId(),
									liveStream.getStreamType(), packetType);
						}
						
					} else {
						LOGGER.warn("livestream not found err: packetSender={}, packetType={}", packetSender, packetType);
					}
				}
			});
			
		} catch (RejectedExecutionException rejectException) {	
			LOGGER.warn("process thread pool is full, reject, active={} poolSize={} corePoolSize={} maxPoolSize={} taskCount={}",
					threadPoolExecutor.getActiveCount(), threadPoolExecutor.getPoolSize(),
					threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getMaximumPoolSize(),
					threadPoolExecutor.getTaskCount());						
		}		
	}
	
}