/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.bigconnect.ffmpeg;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.collection.Pair;
import com.mware.ge.type.GeoPoint;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.probe.FFmpegProbeResult;
import net.bramp.ffmpeg.probe.FFmpegStream;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AVMediaInfo {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(AVMediaInfo.class);

    public static FFmpegProbeResult probe(String absolutePath) {
        try {
            FFprobe ffProbe = AVUtils.ffprobe();
            return ffProbe.probe(absolutePath);
        } catch (IOException e) {
            LOGGER.error("exception running ffprobe", e);
        }

        return null;
    }

    public static boolean hasVideoStream(FFmpegProbeResult probeResult) {
        List<FFmpegStream> streams = probeResult.getStreams();
        if (streams != null) {
            for (FFmpegStream stream : streams) {
                if (stream.codec_type == FFmpegStream.CodecType.VIDEO)
                    return true;
            }
        }
        return false;
    }

    public static boolean hasAudioStream(FFmpegProbeResult probeResult) {
        List<FFmpegStream> streams = probeResult.getStreams();
        if (streams != null) {
            for (FFmpegStream stream : streams) {
                if (stream.codec_type == FFmpegStream.CodecType.AUDIO)
                    return true;
            }
        }
        return false;
    }

    public static VideoFormat getVideoFormat(FFmpegProbeResult probeResult) {
        if (!hasVideoStream(probeResult))
            return null;

        switch (probeResult.getFormat().format_name) {
            case "matroska,webm":
                return VideoFormat.WEBM;
            case "mov,mp4,m4a,3gp,3g2,mj2":
                return VideoFormat.MP4;
            default:
                return VideoFormat.UNKNOWN;
        }
    }

    public static AudioFormat getAudioFormat(FFmpegProbeResult probeResult) {
        if (!hasAudioStream(probeResult))
            return null;

        switch (probeResult.getFormat().format_name) {
            case "matroska,webm":
                return AudioFormat.WEBM;
            case "ogg":
                return AudioFormat.OGG;
            case "mov,mp4,m4a,3gp,3g2,mj2":
                return AudioFormat.MP4;
            case "mp3":
                return AudioFormat.MP3;
            case "wav":
                return AudioFormat.WAV;
            default:
                return AudioFormat.UNKNOWN;
        }
    }

    public static VideoCodec getVideoCodec(FFmpegProbeResult probeResult) {
        if (!hasVideoStream(probeResult))
            return null;

        for (FFmpegStream stream : probeResult.getStreams()) {
            if (stream.codec_type == FFmpegStream.CodecType.VIDEO)
                return parseVideoCodec(stream.codec_name);
        }

        return null;
    }

    public static AudioCodec getAudioCodec(FFmpegProbeResult probeResult) {
        if (!hasAudioStream(probeResult))
            return null;

        for (FFmpegStream stream : probeResult.getStreams()) {
            if (stream.codec_type == FFmpegStream.CodecType.AUDIO)
                return parseAudioCodec(stream.codec_name);
        }

        return null;
    }

    public static boolean isVideoPlayable(FFmpegProbeResult probeResult) {
        if (!hasVideoStream(probeResult))
            return false;

        // https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Video_codecs
        VideoFormat format = getVideoFormat(probeResult);
        VideoCodec codec = getVideoCodec(probeResult);

        return format != VideoFormat.UNKNOWN && codec != VideoCodec.UNKNOWN;
    }

    public static boolean isAudioPlayable(FFmpegProbeResult probeResult) {
        if (!hasAudioStream(probeResult))
            return false;

        // https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Audio_codecs
        AudioFormat format = getAudioFormat(probeResult);
        AudioCodec codec = getAudioCodec(probeResult);

        return format != AudioFormat.UNKNOWN && codec != AudioCodec.UNKNOWN;
    }

    private static VideoCodec parseVideoCodec(String codec_name) {
        if (StringUtils.isEmpty(codec_name))
            return null;

        switch (codec_name) {
            case "av1":
                return VideoCodec.AV1;
            case "h264":
                return VideoCodec.H264;
            case "theora":
                return VideoCodec.THEORA;
            case "vp8":
                return VideoCodec.VP8;
            case "vp9":
                return VideoCodec.VP9;
            default:
                return VideoCodec.UNKNOWN;
        }
    }

    private static AudioCodec parseAudioCodec(String codec_name) {
        if (StringUtils.isEmpty(codec_name))
            return null;

        switch (codec_name) {
            case "aac":
                return AudioCodec.AAC;
            case "flac":
                return AudioCodec.FLAC;
            case "pcm_alaw":
            case "pcm_mulaw":
                return AudioCodec.G711;
            case "adpcm_g722":
                return AudioCodec.G722;
            case "mp3":
                return AudioCodec.MP3;
            case "opus":
                return AudioCodec.OPUS;
            case "vorbis":
                return AudioCodec.VORBIS;
            default:
                if (codec_name.startsWith("pcm_"))
                    return AudioCodec.PCM;
                else
                    return AudioCodec.UNKNOWN;
        }
    }

    public static Pair<Integer, Integer> getDimensions(FFmpegProbeResult probeResult) {
        if (probeResult == null) {
            return null;
        }

        List<FFmpegStream> streams = probeResult.getStreams();
        if (streams != null) {
            for (FFmpegStream stream : streams) {
                if (stream.codec_type == FFmpegStream.CodecType.VIDEO)
                    return Pair.of(stream.width, stream.height);
            }
        }

        return null;
    }

    public static ZonedDateTime getDateTaken(FFmpegProbeResult ffprobe) {
        if (ffprobe == null) {
            return null;
        }

        Map<String, String> tags = ffprobe.getFormat().tags;
        if (tags != null) {
            String dateTaken = null;
            String optionalDateTaken = tags.get("date");
            if (!StringUtils.isEmpty(optionalDateTaken)) {
                dateTaken = optionalDateTaken;
            } else {
                String optionalDateTakenEng = tags.get("date-eng");
                if (!StringUtils.isEmpty(optionalDateTakenEng)) {
                    dateTaken = optionalDateTakenEng;
                }
            }

            if (dateTaken != null && !dateTaken.equals("")) {
                Date date = parseDateTakenString(dateTaken);
                if (date != null) {
                    return ZonedDateTime.ofInstant(date.toInstant(), ZoneOffset.systemDefault());
                }
            }
        }

        LOGGER.debug("Could not extract dateTaken from json.");
        return null;
    }

    public static GeoPoint getGeoPoint(FFmpegProbeResult probeResult) {
        if (probeResult == null) {
            return null;
        }

        Map<String, String> tags = probeResult.getFormat().tags;
        if (tags != null) {
            String locationString = tags.get("location");
            if (!StringUtils.isEmpty(locationString)) {
                GeoPoint geoPoint = parseGeoLocationString(locationString);
                if (geoPoint != null) {
                    return geoPoint;
                }
            }
        }


        LOGGER.debug("Could not retrieve a \"geoLocation\" value from the JSON object.");
        return null;
    }

    private static GeoPoint parseGeoLocationString(String locationString) {
        String myRegularExpression = "(\\+|\\-|/)";
        String[] tempValues = locationString.split(myRegularExpression);
        String[] values = removeNullsFromStringArray(tempValues);
        if (values.length < 2) {
            return null;
        }

        String latitudeValue = values[0];
        String latitudeSign = "";
        int indexOfLatitude = locationString.indexOf(latitudeValue);
        if (indexOfLatitude != 0) {
            latitudeSign = locationString.substring(0, 1);
        }
        String latitudeString = latitudeSign + latitudeValue;
        Double latitude = Double.parseDouble(latitudeString);

        String longitudeValue = values[1];
        String longitudeSign = "";
        int indexOfLongitude = locationString.indexOf(longitudeValue, indexOfLatitude + latitudeValue.length());
        String longitudePreviousChar = locationString.substring(indexOfLongitude - 1, indexOfLongitude);
        if (longitudePreviousChar.equals("-") || longitudePreviousChar.equals("+")) {
            longitudeSign = longitudePreviousChar;
        }
        String longitudeString = longitudeSign + longitudeValue;
        Double longitude = Double.parseDouble(longitudeString);

        String altitudeValue = null;
        Double altitude = null;
        if (values.length == 3) {
            altitudeValue = values[2];
            String altitudeSign = "";
            int indexOfAltitude = locationString.indexOf(altitudeValue, indexOfLongitude + longitudeValue.length());
            String altitudePreviousChar = locationString.substring(indexOfAltitude - 1, indexOfAltitude);
            if (altitudePreviousChar.equals("-") || altitudePreviousChar.equals("+")) {
                altitudeSign = altitudePreviousChar;
            }
            String altitudeString = altitudeSign + altitudeValue;
            altitude = Double.parseDouble(altitudeString);
        }

        if (latitude != null && longitude != null && altitude != null) {
            return new GeoPoint(latitude, longitude, altitude);
        } else if (latitude != null && longitude != null && altitude == null) {
            return new GeoPoint(latitude, longitude);
        } else {
            return null;
        }
    }

    public static String[] removeNullsFromStringArray(String[] array) {
        ArrayList<String> arrayList = new ArrayList<String>();
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null && !array[i].equals("")) {
                arrayList.add(array[i]);
            }
        }
        String[] newArray = arrayList.toArray(new String[arrayList.size()]);
        return newArray;
    }

    private static Date parseDateTakenString(String dateTaken) {
        String dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ";
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        try {
            Date parsedDate = format.parse(dateTaken);
            return parsedDate;
        } catch (ParseException e) {
            LOGGER.debug("ParseException: could not parse dateTaken: " + dateTaken + " with date format: " + dateFormat);
        }

        return null;
    }
}
