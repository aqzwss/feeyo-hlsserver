package com.feeyo.hls.ts.segmenter;

import java.util.ArrayDeque;

import com.feeyo.hls.ts.segmenter.H264TsSegmenter.AvcResult;
import com.feeyo.mpeg2ts.TsWriter;
import com.feeyo.mpeg2ts.TsWriter.FrameData;
import com.feeyo.mpeg2ts.TsWriter.FrameDataType;
import com.feeyo.net.udp.packet.V5PacketType;

// 混合流
public class AacH264MixedTsSegmenter extends AbstractTsSegmenter {
	
	private ArrayDeque<FrameData> avcFrameCache = new ArrayDeque<FrameData>();
	private ArrayDeque<AvcResult> avcFrameWaitDeque = new ArrayDeque<AvcResult>();
	private ArrayDeque<FrameData> aacFrameCache = new ArrayDeque<FrameData>();
	
	private TsWriter tsWriter;
	
	private H264TsSegmenter h264TsSegmenter;
	private AacTsSegmenter aacTsSegmenter;
	
	private byte[][] tsSecs;
	private int tsSecsPtr = 0;
	private int tsSegmentLen = 0;
	
	private boolean isAacFirstPes = true;
	private boolean isAvcFirstPes = true;
	
	private boolean isLastFrame = false;
	
	private long lastAacPts = 0;
	private long lastAvcPts = 0;
	
	public AacH264MixedTsSegmenter() {
		
		tsSecs = new byte[3000][];
		tsWriter = new TsWriter();
		h264TsSegmenter = new H264TsSegmenter();
		aacTsSegmenter = new AacTsSegmenter();
	}

	@Override
	public void initialize(float sampleRate, int sampleSizeInBits, int channels, int fps) {
		h264TsSegmenter.initialize(sampleRate, sampleSizeInBits, channels, fps);
		aacTsSegmenter.initialize(sampleRate, sampleSizeInBits, channels, fps);
	}

	@Override
	protected byte[] transcoding(byte rawDataType, byte[] rawData) {
		return rawData;
	}

	
	//TODO rt
	@Override
	protected byte[] segment(byte rawDataType, byte[] rawData) {
		switch(rawDataType) {
		case V5PacketType.AAC_STREAM:
			FrameData aacFrame = aacTsSegmenter.process(rawData);
			if(aacFrame != null) 
				aacFrameCache.offer(aacFrame);
			
			break;
			
		case V5PacketType.H264_STREAM:
			AvcResult avcResult = h264TsSegmenter.process(rawData);
			if(avcResult != null) {
				
				avcFrameWaitDeque.offer(avcResult);
				
				if(isLastFrame) {
					
					if(tsSecs[0] == null)
						return null;
					//write2Ts
					long maxPts = lastAacPts > lastAvcPts ? lastAacPts : lastAvcPts;
					tsSegTime = (maxPts - ptsBase) / 90000F;
					ptsBase = maxPts;
					byte[] tsSegment = new byte[tsSegmentLen];
	                int tsSegmentPtr = 0;
					for (int i = 0; i < tsSecs.length; i++) {
						if(tsSecs[i] != null) {
		                    System.arraycopy(tsSecs[i], 0, tsSegment, tsSegmentPtr, tsSecs[i].length);
		                    tsSegmentPtr += tsSecs[i].length;
						}
	                }
	                prepare4nextTs();
	                
	                return tsSegment;
					
				}else {
				
					while(!isLastFrame) {
						if(avcFrameWaitDeque.isEmpty())
							break;
						avcResult = avcFrameWaitDeque.peek();
						writeFrame();
						
						if(avcResult.isLastFrame) {
							isLastFrame = true;
							
							while(!avcFrameCache.isEmpty()) {
								FrameData frameData = avcFrameCache.pop();
								byte[] aacTsSegment= tsWriter.write(isAvcFirstPes,  FrameDataType.MIXED, frameData);
								
								if(aacTsSegment!= null) {
									tsSegmentLen += aacTsSegment.length;
						            tsSecs[tsSecsPtr++] = aacTsSegment;
								}
								isAvcFirstPes = false;
								lastAvcPts = frameData.pts;
							}
							
							//write2Ts
							long maxPts = lastAacPts > lastAvcPts ? lastAacPts : lastAvcPts;
							tsSegTime = (maxPts - ptsBase) / 90000F;
							ptsBase = maxPts;
							byte[] tsSegment = new byte[tsSegmentLen];
			                int tsSegmentPtr = 0;
							for (int i = 0; i < tsSecs.length; i++) {
								if(tsSecs[i] != null) {
				                    System.arraycopy(tsSecs[i], 0, tsSegment, tsSegmentPtr, tsSecs[i].length);
				                    tsSegmentPtr += tsSecs[i].length;
								}
			                }
			                prepare4nextTs();
			                return tsSegment;
						}
					}
				}
			}
			break;
		}
		
		return null;
	}
	
	private void writeFrame() {
		
		if(!isLastFrame) {
			
			while(!avcFrameWaitDeque.isEmpty()) {
				AvcResult result = avcFrameWaitDeque.pop();
				for(FrameData avcFrame : result.avcFrames)
					avcFrameCache.offer(avcFrame);
				if(!result.isLastFrame)
					break;
			}
			
			while( !aacFrameCache.isEmpty() && !avcFrameCache.isEmpty()) {
				
				FrameData avcFrame = avcFrameCache.peek();
				if(aacFrameCache.peek().pts < avcFrame.pts) {
					FrameData frameData = aacFrameCache.pop();
					byte[] aacTsSegment= tsWriter.write(isAacFirstPes,  FrameDataType.MIXED, frameData);
					
					if(aacTsSegment!= null) {
						tsSegmentLen += aacTsSegment.length;
			            tsSecs[tsSecsPtr++] = aacTsSegment;
					}
					
					isAacFirstPes = false;
					lastAacPts = frameData.pts;
				}else {
					
					FrameData frameData = avcFrameCache.pop();
					byte[] avcTsSegment= tsWriter.write(isAvcFirstPes,  FrameDataType.MIXED, frameData);
					if(avcTsSegment != null) {
						tsSegmentLen += avcTsSegment.length;
			            tsSecs[tsSecsPtr++] = avcTsSegment;
					}
					
					isAvcFirstPes = false;
					lastAvcPts = frameData.pts;
				}
			}
		}
	}
			
	public void prepare4nextTs() {
		isAvcFirstPes = true;
		isAacFirstPes = true;
		isLastFrame = false;
		
		tsSegmentLen = 0;
		tsSecsPtr = 0;
		tsWriter.reset();
		h264TsSegmenter.prepare4nextTs();
		aacTsSegmenter.prepare4nextTs();
		
		for (int i = 0; i < tsSecs.length; i++) {
			tsSecs[i] = null;
		}
		
	}

	@Override
	public void close() {
		
		isAvcFirstPes = true;
		isAacFirstPes = true;
		isLastFrame = false;
		
		tsSegmentLen = 0;
		tsSecsPtr = 0;
		for (int i = 0; i < tsSecs.length; i++) {
			tsSecs[i] = null;
		}
	}
}
