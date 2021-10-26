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
package com.mware.dw.url;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.DataWorkerPrepareData;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ExistingElementMutation;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.json.JsonObject;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Name("Facebook URL Engagement")
@Description("Get engagement for an URL from Facebook")
public class FacebookUrlEngagementWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(FacebookUrlEngagementWorker.class);
    private static final String CONFIG_FB_APPID = FacebookUrlEngagementWorker.class.getName()+".appId";
    private static final String CONFIG_FB_APPSECRET = FacebookUrlEngagementWorker.class.getName()+".appSecret";

    private String appId;
    private String appSecret;

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        appId = getConfiguration().get(CONFIG_FB_APPID, "");
        appSecret = getConfiguration().get(CONFIG_FB_APPSECRET, "");

        if (StringUtils.isEmpty(appId))
            throw new BcException("Facebook App ID is required for this data worker. Please set property "
                    + CONFIG_FB_APPID + " in the config file");

        if (StringUtils.isEmpty(appSecret))
            throw new BcException("Facebook App Secret is required for this data worker. Please set property "
                    + CONFIG_FB_APPSECRET + " in the config file");
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        Element element = refresh(data.getElement());
        String url = RawObjectSchema.URL.getPropertyValue(element);
        if (StringUtils.isEmpty(url))
            return;

        if (!StringUtils.startsWithIgnoreCase(url, "http://") && !StringUtils.startsWithIgnoreCase(url, "https://")) {
            // assume http
            url = "http://"+url;
        }

        FacebookClient fbClient = new DefaultFacebookClient(Version.VERSION_3_3);
        FacebookClient.AccessToken accessToken = fbClient.obtainAppAccessToken(appId, appSecret);
        fbClient = new DefaultFacebookClient(accessToken.getAccessToken(), Version.VERSION_3_3);

        JsonObject results = fbClient.fetchObject(url, JsonObject.class, Parameter.with("fields", "engagement"));
        if (results != null && results.contains("engagement")) {
            JsonObject engagement = (JsonObject) results.get("engagement");
            int reactionCount = engagement.getInt("reaction_count", 0);
            int commentCount = engagement.getInt("comment_count", 0);
            int shareCount = engagement.getInt("share_count", 0);

            ExistingElementMutation m = element.prepareMutation();
            List<BcPropertyUpdate> changedProperties = new ArrayList<>();

            Metadata metadata = data.createPropertyMetadata(getUser());
            RawObjectSchema.LIKES.updateProperty(changedProperties, element, m, reactionCount, metadata, element.getVisibility());
            RawObjectSchema.SHARES.updateProperty(changedProperties, element, m, shareCount, metadata, element.getVisibility());
            RawObjectSchema.COMMENTS.updateProperty(changedProperties, element, m, commentCount, metadata, element.getVisibility());
            element = m.save(getAuthorizations());

            getWebQueueRepository().broadcastPropertiesChange(
                    element,
                    changedProperties,
                    data.getWorkspaceId(),
                    data.getPriority()
            );

            getWorkQueueRepository().pushOnDwQueue(
                    element,
                    changedProperties,
                    data.getWorkspaceId(),
                    data.getVisibilitySource(),
                    data.getPriority()
            );
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (!property.getName().equals(RawObjectSchema.URL.getPropertyName())) {
            return false;
        }

        if (!isVertex(element))
            return false;

        String conceptType = ((Vertex)element).getConceptType();
        if (!SchemaConstants.CONCEPT_TYPE_DOCUMENT.equals(conceptType))
            return false;

        return !StringUtils.isEmpty(
                RawObjectSchema.URL.getPropertyValue(element)
        );
    }
}
