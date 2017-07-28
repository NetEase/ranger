/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.rest;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.httpclient.URI;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.*;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.*;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.*;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.util.RangerRestUtil;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Path("plugins")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ServiceREST {
	private static final Log LOG = LogFactory.getLog(ServiceREST.class);
	private static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("rest.ServiceREST");

	private static final String EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
	private static final String POLICY_DESC_TABLE_TYPE = "table_type";
	private static final String POLICY_DESC_TABLE_LOCATION = "table_location";

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	ServiceMgr serviceMgr;

	@Autowired
	AssetMgr assetMgr;

	@Autowired
	XUserMgr userMgr;

	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	ServiceUtil serviceUtil;
	
	@Autowired
	RangerConfigUtil configUtil;
	
	@Autowired
	RangerPolicyService policyService;
	
	@Autowired
	RangerServiceService svcService;
	
	@Autowired
	RangerServiceDefService serviceDefService;
	
	@Autowired
	RangerSearchUtil searchUtil;
	
    @Autowired
    RangerBizUtil bizUtil;

	@Autowired
	GUIDUtil guidUtil;
	
	@Autowired
	RangerValidatorFactory validatorFactory; 

	@Autowired
	RangerDaoManager daoManager;

	public ServiceREST() {
	}

	@POST
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE_DEF + "\")")
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createServiceDef(serviceDefName=" + serviceDef.getName() + ")");
		}

		try {
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.CREATE);

			bizUtil.hasAdminPermissions("Service-Def");
			bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());

			ret = svcStore.createServiceDef(serviceDef);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createServiceDef(" + serviceDef + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE_DEF + "\")")
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateServiceDef(serviceDefName=" + serviceDef.getName() + ")");
		}

		try {
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.UPDATE);

			bizUtil.hasAdminPermissions("Service-Def");
			bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());

			ret = svcStore.updateServiceDef(serviceDef);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updateServiceDef(" + serviceDef + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE_DEF + "\")")
	public void deleteServiceDef(@PathParam("id") Long id, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteServiceDef(" + id + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteServiceDef(serviceDefId=" + id + ")");
		}

		try {
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(id, Action.DELETE);

			bizUtil.hasAdminPermissions("Service-Def");
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);
			bizUtil.hasKMSPermissions("Service-Def", xServiceDef.getImplclassname());

			String forceDeleteStr = request.getParameter("forceDelete");
			boolean forceDelete = false;
			if(!StringUtils.isEmpty(forceDeleteStr) && forceDeleteStr.equalsIgnoreCase("true")) {
				forceDelete = true;
			}
			
			svcStore.deleteServiceDef(id, forceDelete);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deleteServiceDef(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteServiceDef(" + id + ")");
		}
	}

	@GET
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF + "\")")
	public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDef(serviceDefId=" + id + ")");
		}

		try {
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);
			if (!bizUtil.hasAccess(xServiceDef, null)) {
				throw restErrorUtil.createRESTException(
						"User is not allowed to access service-def, id: " + xServiceDef.getId(),
						MessageEnums.OPER_NO_PERMISSION);
			}

			ret = svcStore.getServiceDef(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceDef(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/definitions/name/{name}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF_BY_NAME + "\")")
	public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefByName(serviceDefName=" + name + ")");
		}

		try {
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().findByName(name);
			if (xServiceDef != null) {
				if (!bizUtil.hasAccess(xServiceDef, null)) {
					throw restErrorUtil.createRESTException(
							"User is not allowed to access service-def: " + xServiceDef.getName(),
							MessageEnums.OPER_NO_PERMISSION);
				}
			}

			ret = svcStore.getServiceDefByName(name);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceDefByName(" + name + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefByName(" + name + "): " + ret);
		}

		return ret;
	}
	
	@GET
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEFS + "\")")
	public RangerServiceDefList getServiceDefs(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefs()");
		}

		RangerServiceDefList ret  = null;
		RangerPerfTracer     perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefs()");
		}

		SearchFilter filter = searchUtil.getSearchFilter(request, serviceDefService.sortFields);

		try {
			ret = svcStore.getPaginatedServiceDefs(filter);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServiceDefs() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefs(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	@POST
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE + "\")")
	public RangerService createService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createService(" + service + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createService(serviceName=" + service.getName() + ")");
		}

		try {
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.CREATE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			ret = svcStore.createService(service);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createService(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createService(" + service + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE + "\")")
	public RangerService updateService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateService(): " + service);
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateService(serviceName=" + service.getName() + ")");
		}

		try {
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.UPDATE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			ret = svcStore.updateService(service);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updateService(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateService(" + service + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE + "\")")
	public void deleteService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteService(" + id + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteService(serviceId=" + id + ")");
		}

		try {
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(id, Action.DELETE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXService service = daoManager.getXXService().getById(id);
			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().getById(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			svcStore.deleteService(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deleteService(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteService(" + id + ")");
		}
	}

	@GET
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE + "\")")
	public RangerService getService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getService(" + id + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceId=" + id + ")");
		}

		try {
			ret = svcStore.getService(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getService(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getService(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/services/name/{name}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_BY_NAME + "\")")
	public RangerService getServiceByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceByName(" + name + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceName=" + name + ")");
		}

		try {
			ret = svcStore.getServiceByName(name);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceByName(" + name + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceByName(" + name + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICES + "\")")
	public RangerServiceList getServices(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices()");
		}

		RangerServiceList ret  = null;
		RangerPerfTracer  perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
		}

		SearchFilter filter = searchUtil.getSearchFilter(request, svcService.sortFields);

		try {
			ret = svcStore.getPaginatedServices(filter);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServices(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	public List<RangerService> getServices(SearchFilter filter) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices():");
		}

		List<RangerService> ret  = null;
		RangerPerfTracer    perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
		}

		try {
			ret = svcStore.getServices(filter);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServices(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}


	@GET
	@Path("/services/count")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_SERVICES + "\")")
	public Long countServices(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.countServices():");
		}

		Long             ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countService()");
		}

		try {
			List<RangerService> services = getServices(request).getServices();
			
			ret = new Long(services == null ? 0 : services.size());
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("countServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countServices(): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/validateConfig")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.VALIDATE_CONFIG + "\")")
	public VXResponse validateConfig(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.validateConfig(" + service + ")");
		}

		VXResponse       ret  = new VXResponse();
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.validateConfig(serviceName=" + service.getName() + ")");
		}

		try {
			ret = serviceMgr.validateConfig(service, svcStore);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("validateConfig(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validateConfig(" + service + "): " + ret);
		}

		return ret;
	}
	
	@POST
	@Path("/services/lookupResource/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LOOKUP_RESOURCE + "\")")
	public List<String> lookupResource(@PathParam("serviceName") String serviceName, ResourceLookupContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.lookupResource(" + serviceName + ")");
		}

		List<String>     ret  = new ArrayList<String>();
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.lookupResource(serviceName=" + serviceName + ")");
		}

		try {

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.lookupResource(serviceName=" + serviceName + ")");
			}
			ret = serviceMgr.lookupResource(serviceName, context, svcStore);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("lookupResource(" + serviceName + ", " + context + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.lookupResource(" + serviceName + "): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/grant/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse grantAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest grantRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.grantAccess(" + serviceName + ", " + grantRequest + ")");
		}

		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.grantAccess(serviceName=" + serviceName + ")");
		}
			
		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {

			try {
				String               userName   = grantRequest.getGrantor();
				Set<String>          userGroups = userMgr.getGroupsForUser(userName);
				RangerAccessResource resource   = new RangerAccessResourceImpl(grantRequest.getResource());
	
				boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);
	
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource);
		
				if(policy != null) {
					boolean policyUpdated = false;
	
					// replace all existing privileges for users and groups
					if(grantRequest.getReplaceExistingPermissions()) {
						policyUpdated = removeUsersAndGroupsFromPolicy(policy, grantRequest.getUsers(), grantRequest.getGroups());
					}
	
					for(String user : grantRequest.getUsers()) {
						RangerPolicyItem policyItem = getPolicyItemForUser(policy, user);
						
						if(policyItem != null) {
							if(addAccesses(policyItem, grantRequest.getAccessTypes())) {
								policyUpdated = true;
							}
						} else {
							policyItem = new RangerPolicyItem();
							
							policyItem.getUsers().add(user);
							addAccesses(policyItem, grantRequest.getAccessTypes());
							policy.getPolicyItems().add(policyItem);
	
							policyUpdated = true;
						}
	
						if(grantRequest.getDelegateAdmin()) {
							if(!policyItem.getDelegateAdmin()) {
								policyItem.setDelegateAdmin(Boolean.TRUE);
		
								policyUpdated = true;
							}
						}
					}
	
					for(String group : grantRequest.getGroups()) {
						RangerPolicyItem policyItem = getPolicyItemForGroup(policy, group);
						
						if(policyItem != null) {
							if(addAccesses(policyItem, grantRequest.getAccessTypes())) {
								policyUpdated = true;
							}
						} else {
							policyItem = new RangerPolicyItem();
							
							policyItem.getGroups().add(group);
							addAccesses(policyItem, grantRequest.getAccessTypes());
							policy.getPolicyItems().add(policyItem);
	
							policyUpdated = true;
						}
	
						if(grantRequest.getDelegateAdmin()) {
							if(!policyItem.getDelegateAdmin()) {
								policyItem.setDelegateAdmin(Boolean.TRUE);
		
								policyUpdated = true;
							}
						}
					}
	
					if(policyUpdated) {
						svcStore.updatePolicy(policy);
					}
				} else {
					policy = new RangerPolicy();
					policy.setService(serviceName);
					policy.setName("grant-" + System.currentTimeMillis()); // TODO: better policy name
					policy.setDescription("created by grant");
					policy.setIsAuditEnabled(grantRequest.getEnableAudit());
					policy.setCreatedBy(userName);
		
					Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
					Set<String>                       resourceNames   = resource.getKeys();
		
					if(! CollectionUtils.isEmpty(resourceNames)) {
						for(String resourceName : resourceNames) {
							RangerPolicyResource policyResource = new RangerPolicyResource(resource.getValue(resourceName));
							policyResource.setIsRecursive(grantRequest.getIsRecursive());
	
							policyResources.put(resourceName, policyResource);
						}
					}
					policy.setResources(policyResources);
		
					for(String user : grantRequest.getUsers()) {
						RangerPolicyItem policyItem = new RangerPolicyItem();
			
						policyItem.getUsers().add(user);
						for(String accessType : grantRequest.getAccessTypes()) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
						}
						policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
						policy.getPolicyItems().add(policyItem);
					}
					
					for(String group : grantRequest.getGroups()) {
						RangerPolicyItem policyItem = new RangerPolicyItem();
			
						policyItem.getGroups().add(group);
						for(String accessType : grantRequest.getAccessTypes()) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
						}
						policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
						policy.getPolicyItems().add(policyItem);
					}
		
					svcStore.createPolicy(policy);
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Throwable excp) {
				LOG.error("grantAccess(" + serviceName + ", " + grantRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(excp.getMessage());
			} finally {
				RangerPerfTracer.log(perf);
			}
	
			ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.grantAccess(" + serviceName + ", " + grantRequest + "): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/revoke/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse revokeAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest revokeRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.revokeAccess(" + serviceName + ", " + revokeRequest + ")");
		}

		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.revokeAccess(serviceName=" + serviceName + ")");
		}

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {

			try {
				String               userName     = revokeRequest.getGrantor();
				Set<String>          userGroups   =  userMgr.getGroupsForUser(userName);
				RangerAccessResource resource     = new RangerAccessResourceImpl(revokeRequest.getResource());

				boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);
				
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource);
				
				if(policy != null) {
					boolean policyUpdated = false;

					// remove all existing privileges for users and groups
					if(revokeRequest.getReplaceExistingPermissions()) {
						policyUpdated = removeUsersAndGroupsFromPolicy(policy, revokeRequest.getUsers(), revokeRequest.getGroups());
					} else {
						for(String user : revokeRequest.getUsers()) {
							RangerPolicyItem policyItem = getPolicyItemForUser(policy, user);

							if (policyItem != null) {
								if (removeAccesses(policyItem, revokeRequest.getAccessTypes())) {
									policyUpdated = true;
								}

								if (revokeRequest.getDelegateAdmin()) { // remove delegate?
									if (policyItem.getDelegateAdmin()) {
										policyItem.setDelegateAdmin(Boolean.FALSE);
										policyUpdated = true;
									}

								}
							}
						}
	
						for(String group : revokeRequest.getGroups()) {
							RangerPolicyItem policyItem = getPolicyItemForGroup(policy, group);
						
							if(policyItem != null) {
								if(removeAccesses(policyItem, revokeRequest.getAccessTypes())) {
									policyUpdated = true;
								}

								if(revokeRequest.getDelegateAdmin()) { // remove delegate?
									if(policyItem.getDelegateAdmin()) {
										policyItem.setDelegateAdmin(Boolean.FALSE);
										policyUpdated = true;
									}
								}
							}
						}
	
						if(compactPolicy(policy)) {
							policyUpdated = true;
						}
					}
	
					if(policyUpdated) {
						svcStore.updatePolicy(policy);
					}
				} else {
					// nothing to revoke!
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Throwable excp) {
				LOG.error("revokeAccess(" + serviceName + ", " + revokeRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(excp.getMessage());
			} finally {
				RangerPerfTracer.log(perf);
			}
	
			ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.revokeAccess(" + serviceName + ", " + revokeRequest + "): " + ret);
		}

		return ret;
	}

	private void mockSession(HttpServletRequest request, String userName) {
		String ipAddress = request.getHeader("X-FORWARDED-FOR");
		if (ipAddress == null) {
			ipAddress = request.getRemoteAddr();
		}

		Long userId = userMgr.getXUserByUserName(userName).getId();

		XXAuthSession gjAuthSession = new XXAuthSession();
		gjAuthSession.setLoginId("");
		gjAuthSession.setUserId(userId);
		gjAuthSession.setAuthTime(DateUtil.getUTCDate());
		gjAuthSession.setAuthStatus(XXAuthSession.AUTH_STATUS_SUCCESS);
		gjAuthSession.setAuthType(XXAuthSession.AUTH_TYPE_UNKNOWN);
		gjAuthSession.setDeviceType(RangerCommonEnums.DEVICE_UNKNOWN);
		gjAuthSession.setExtSessionId(null);
		gjAuthSession.setRequestIP(ipAddress);
		gjAuthSession.setRequestUserAgent(null);

		//gjAuthSession = storeAuthSession(gjAuthSession);

		XXPortalUser portalUser = daoManager.getXXPortalUser().findByXUserId(userId);

		UserSessionBase userSession = new UserSessionBase();
		userSession.setXXPortalUser(portalUser);
		userSession.setXXAuthSession(gjAuthSession);
		userSession.setUserAdmin(true);

		// create context with user-session and set in thread-local
		RangerSecurityContext context = new RangerSecurityContext();
		context.setUserSession(userSession);
		RangerContextHolder.setSecurityContext(context);
	}

	@POST
	@Path("/policies/synchronize/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse synchronizePolicy(@PathParam("serviceName") String serviceName,
																			@QueryParam("pluginId") String pluginId,
																			@QueryParam("hiveOperationType") HiveOperationType hiveOperationType,
																				SynchronizeRequest syncRequest,
																		  @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.synchronizePolicy(" + serviceName + ", " + syncRequest + ")");
		}

		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.synchronizePolicy(serviceName=" + serviceName + ")");
		}

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
			try {
				String               userName   = syncRequest.getGrantor();
				Set<String>          userGroups = userMgr.getGroupsForUser(userName);
				RangerAccessResource resource   = new RangerAccessResourceImpl(syncRequest.getResource());
				syncRequest.setGroups(null); // Generate user rights only

				boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);
				if(!isAdmin) {
					LOG.warn("hasAdminAccess(" + serviceName + ", " + userName + ", " + userGroups + ", "
							+ syncRequest + ") unauthorized or not delegateadmin!");
				} else {
					mockSession(request, userName);
					syncCatlog(serviceName, hiveOperationType, syncRequest);
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Throwable excp) {
				LOG.error("consistentRules(" + serviceName + ", " + syncRequest + ") failed", excp);
				throw restErrorUtil.createRESTException(excp.getMessage());
			} finally {
				RangerPerfTracer.log(perf);
			}

			ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.synchronizePolicy(" + serviceName + ", " + syncRequest + "): " + ret);
		}

		return ret;
	}

	private RangerPolicy searchHivePolicy(Long hiveServiceId, String dbName, String tabName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.searchHivePolicy(" + dbName + ", " + tabName + ") ");
		}

		SearchFilter hiveFilter = new SearchFilter();
		hiveFilter.setParam(SearchFilter.SERVICE_ID, hiveServiceId.toString());
		hiveFilter.setParam(SearchFilter.SERVICE_TYPE, "hive");
		hiveFilter.setParam(SearchFilter.RESOURCE_PREFIX + "database", dbName);
		hiveFilter.setParam(SearchFilter.RESOURCE_PREFIX + "table", tabName);
		hiveFilter.setParam(SearchFilter.RESOURCE_PREFIX + "column", "*");
		List<RangerPolicy> macthHivePolicies = getPolicies(hiveFilter);
		for (RangerPolicy policy : macthHivePolicies) {
			RangerPolicyResource dbResource = policy.getResources().get("database");
			RangerPolicyResource tabResource = policy.getResources().get("table");

			if (dbResource.getValues().containsAll(Arrays.asList(dbName))
					&& tabResource.getValues().containsAll(Arrays.asList(tabName))) {
				return policy;
			}
		}

		return null;
	}

	// search hdfs policy by hdfs location
	// a hdfs policy may correspond to multiple hive policy(.eg Multiple tables have the same location)
	private RangerPolicy searchHdfsPolicyByLocation(Long hdfsServiceId, String location) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.searchHdfsPolicyByLocation(" + hdfsServiceId + ", " + location + ") ");
		}

		URI uri = new URI(location);
		String hdfsPath = uri.getPath();

		SearchFilter hdfsFilter = new SearchFilter();
		hdfsFilter.setParam(SearchFilter.SERVICE_ID, hdfsServiceId.toString());
		hdfsFilter.setParam(SearchFilter.SERVICE_TYPE, "hdfs");
		hdfsFilter.setParam(SearchFilter.RESOURCE_PREFIX + "path", hdfsPath);
		List<RangerPolicy> macthHdfsPolicies = getPolicies(hdfsFilter);
		RangerPolicy hdfsPolicy = null;
		for (RangerPolicy policy : macthHdfsPolicies) {
			if (policy.getResources().get("path") == null) {
				continue;
			}

			List<String> resources = policy.getResources().get("path").getValues();
			if (resources.containsAll(Arrays.asList(hdfsPath))) {
				hdfsPolicy = policy;
				break;
			}
		}

		if (null == hdfsPolicy) {
			LOG.warn("can not find matching hdfs policy " + hdfsServiceId + ", " + location + ") ");
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.searchHdfsPolicyByLocation(" + hdfsServiceId + ", " + location + ") ");
		}

		return hdfsPolicy;
	}

	private RangerPolicy searchHdfsPolicyByHivePolicyName(Long hiveServiceId, String hdfsPolicyName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.searchHdfsPolicyByHivePolicyName(" + hiveServiceId + ", " + hdfsPolicyName + ") ");
		}

		RangerService hdfsService = getRelatedHdfsService(hiveServiceId);
		SearchFilter hdfsFilter = new SearchFilter();
		hdfsFilter.setParam(SearchFilter.SERVICE_NAME, hdfsService.getName());
		hdfsFilter.setParam(SearchFilter.SERVICE_TYPE, "hdfs");
		hdfsFilter.setParam(SearchFilter.POLICY_NAME, hdfsPolicyName);
		List<RangerPolicy> macthHdfsPolicies = getPolicies(hdfsFilter);
		RangerPolicy hdfsPolicy = null;
		for (RangerPolicy policy : macthHdfsPolicies) {
			if (policy.getResources().get("path") == null) {
				continue;
			}

			hdfsPolicy = policy;
			break;

		}

		if (null == hdfsPolicy) {
			LOG.warn("can not find matching hdfs policy " + hiveServiceId + ", " + hdfsPolicyName + ") ");
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.searchHdfsPolicyByHivePolicyName(" + hiveServiceId + ", " + hdfsPolicyName + ") ");
		}

		return hdfsPolicy;
	}

	// search hdfs policy by hdfs location
	// a hdfs policy may correspond to multiple hive policy(.eg Multiple tables have the same location)
	private List<RangerPolicy> searchHivePolicyByLocation(Long hiveServiceId, String location) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.searchHdfsPolicyByLocation(" + hiveServiceId + ", " + location + ") ");
		}

		URI uri = new URI(location);
		String hdfsPath = uri.getPath();

		SearchFilter hdfsFilter = new SearchFilter();
		hdfsFilter.setParam(SearchFilter.SERVICE_ID, hiveServiceId.toString());
		hdfsFilter.setParam(SearchFilter.SERVICE_TYPE, "hive");
		hdfsFilter.setParam(SearchFilter.POLICY_DESC_PREFIX + POLICY_DESC_TABLE_LOCATION, hdfsPath);
		List<RangerPolicy> macthHivePolicies = getPolicies(hdfsFilter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.searchHdfsPolicyByLocation(" + hiveServiceId + ", " + location + ") ");
		}

		return macthHivePolicies;
	}

	private RangerPolicy searchHdfsPolicy(RangerPolicy hivePolicy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.searchHdfsPolicy(" + hivePolicy.getName() + ") ");
		}

		RangerPolicy hdfsPolicy = null;
		SearchFilter hdfsFilter = new SearchFilter();
		hdfsFilter.setParam(SearchFilter.POLICY_NAME, generateHdfsPolicyName(hivePolicy));
		hdfsFilter.setParam(SearchFilter.SERVICE_TYPE, "hdfs");
		List<RangerPolicy> macthHdfsPolicies = getPolicies(hdfsFilter);
		if (macthHdfsPolicies.size() == 1) {
			hdfsPolicy = macthHdfsPolicies.get(0);
		} else if (macthHdfsPolicies.size() > 1) {
			LOG.error("search for more than 2 hdfs policy");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.searchHdfsPolicy(" + hivePolicy.getName() + ") ");
		}
		return hdfsPolicy;
	}

	private boolean userIsDelegateAdmin(RangerPolicy policy, String userName, Set<String> groups) {
		boolean ret = false;
		List<RangerPolicy.RangerPolicyItem> items = policy.getPolicyItems();
		if (CollectionUtils.isNotEmpty(items)) {
			for (RangerPolicy.RangerPolicyItem item : items) {
				if((CollectionUtils.isEmpty(item.getUsers()) && CollectionUtils.isEmpty(item.getGroups()))
						|| CollectionUtils.isEmpty(item.getAccesses()) || !item.getDelegateAdmin()) {
					continue;
				}

				if(item.getUsers().contains(userName) || CollectionUtils.containsAny(item.getGroups(), groups)) {
					ret = true;
					break;
				}
			}
		}

		return ret;
	}

	private RangerPolicy policiesContains(List<RangerPolicy> policies, Long policyId) {
		for (RangerPolicy policy : policies) {
			if (policy.getId() == policyId) {
				return policy;
			}
		}

		return null;
	}

	private void syncCatlog(String matchHiveServiceName, HiveOperationType hiveOperationType,
															SynchronizeRequest syncRequest) throws Exception {
		RangerService service = getServiceByName(matchHiveServiceName);
		if (null == service) {
			LOG.error("service does not exist - name=" + matchHiveServiceName);
			throw new Exception("service does not exist - name = " + matchHiveServiceName);
		}

		String location = syncRequest.getLocation();
		RangerService hdfsService = getRelatedHdfsService(service.getId());
		switch (hiveOperationType) {
			case CREATETABLE: {
				List<RangerPolicy> relatedHivePolicies = new ArrayList<>();
				// update/create hdfs policy
				RangerPolicy newHivePolicy = generateHivePolicy(matchHiveServiceName, syncRequest);
				RangerPolicy relatedHdfsPolicy = searchHdfsPolicyByLocation(hdfsService.getId(), location);
				if (null == relatedHdfsPolicy) {
					relatedHivePolicies.add(newHivePolicy);

					relatedHdfsPolicy = generateHdfsPolicy(relatedHivePolicies, location);
					svcStore.createPolicy(relatedHdfsPolicy);
				} else {
					relatedHivePolicies = searchHivePolicyByLocation(service.getId(), location);
					relatedHivePolicies.add(newHivePolicy);

					RangerPolicy calcHdfsPolicy = generateHdfsPolicy(relatedHivePolicies, location);
					relatedHdfsPolicy.getPolicyItems().clear();
					relatedHdfsPolicy.getPolicyItems().addAll(calcHdfsPolicy.getPolicyItems());
					svcStore.updatePolicy(relatedHdfsPolicy);
				}

				// last update hive Description(hdfsPolicyId)
				URI uri = new URI(location);
				String hdfsPath = uri.getPath();
				setPolicyDesc(newHivePolicy, POLICY_DESC_TABLE_LOCATION, hdfsPath);
				svcStore.createPolicy(newHivePolicy);
			}
				break;
			case DROPTABLE: {
				// delete match hive policy
				RangerAccessResource resource = new RangerAccessResourceImpl(syncRequest.getResource());
				RangerPolicy matchHivePolicy = getExactMatchPolicyForResource(matchHiveServiceName, resource);
				if (null == matchHivePolicy) {
					LOG.warn("can not find matching hive policy " + matchHiveServiceName);
				} else {
					svcStore.deletePolicy(matchHivePolicy.getId());
				}

				RangerPolicy relatedHdfsPolicy = searchHdfsPolicyByLocation(hdfsService.getId(), location);
				if (null != relatedHdfsPolicy) {
					// recalc hdfs policy
					List<RangerPolicy> hivePolicies = searchHivePolicyByLocation(service.getId(), location);
					// the matchHivePolicy has been deleted before
					if (hivePolicies.size() == 0) {
						svcStore.deletePolicy(relatedHdfsPolicy.getId());
					} else {
						RangerPolicy calcHdfsPolicy = generateHdfsPolicy(hivePolicies, location);
						if (calcHdfsPolicy.getPolicyItems().size() == 0) {
							svcStore.deletePolicy(relatedHdfsPolicy.getId());
						} else {
							relatedHdfsPolicy.getPolicyItems().clear();
							relatedHdfsPolicy.getPolicyItems().addAll(calcHdfsPolicy.getPolicyItems());
							svcStore.updatePolicy(relatedHdfsPolicy);
						}
					}
				}
			}
				break;
			case ALTERTABLE: {
				RangerAccessResource resource = new RangerAccessResourceImpl(syncRequest.getResource());
				RangerPolicy matchHivePolicy = getExactMatchPolicyForResource(matchHiveServiceName, resource);
				if (null == matchHivePolicy) {
					LOG.warn("can not find matching hive policy " + matchHiveServiceName);
				}

				String newLocation = syncRequest.getNewLocation();
				if (false == location.equalsIgnoreCase(newLocation)) {
					adjustHdfsPolicyByLocation(service.getId(), location, null, matchHivePolicy);

					// newHdfsPolicy may exist or may not exist
					adjustHdfsPolicyByLocation(service.getId(), newLocation, matchHivePolicy, null);

					// TODO:update all hive-policy Description(hdfs-policy-id, location)
					URI uri = new URI(newLocation);
					String hdfsPath = uri.getPath();
					setPolicyDesc(matchHivePolicy, POLICY_DESC_TABLE_LOCATION, hdfsPath);
					svcStore.updatePolicy(matchHivePolicy);
				} else {
					// update hive-policy db and table name
					RangerPolicy hivePolicy = alterHivePolicyResource(matchHivePolicy, syncRequest);
					svcStore.updatePolicy(hivePolicy);
				}
			}
				break;
			default:
				LOG.error("ServiceREST.syncHdfsPolicy(" + matchHiveServiceName + ") mismatched hive operation " + hiveOperationType.name());
				break;
		}
	}

	// update hive Description
	private void updateHivePolicyDescByTableName(Long hiveServiceId, RangerPolicy hivePolicy) throws Exception {
		RangerPolicyResource dbResource = hivePolicy.getResources().get("database");
		RangerPolicyResource tabResource = hivePolicy.getResources().get("table");

		if (null != dbResource && dbResource.getValues().size() != 1
				&& null != tabResource && tabResource.getValues().size() != 1) {
			return;
		}

		String dbName = dbResource.getValues().get(0);
		String tabName = tabResource.getValues().get(0);
		if (dbName.trim().isEmpty() || dbName.equalsIgnoreCase("*")
				|| tabName.trim().isEmpty() || tabName.equalsIgnoreCase("*")) {
			return;
		}

		RangerPolicy tablePolicy = searchHivePolicy(hiveServiceId, dbName, tabName);
		if (null != tablePolicy) {
			hivePolicy.setDescription(tablePolicy.getDescription());
		}
	}

	private void adjustHdfsPolicyByLocation(Long hiveServiceId, String location,
																				 RangerPolicy addPolicy, RangerPolicy minusPolicy) throws Exception {
		if (location.isEmpty()) {
			LOG.warn("adjustHdfsPolicyByLocation() param location is empty!");
			return;
		}

		RangerService hdfsService = getRelatedHdfsService(hiveServiceId);
		if (null == hdfsService) {
			LOG.error("Related Hdfs Service does not exist - hiveServiceId = " + hiveServiceId);
			throw new Exception("Related Hdfs Service does not exist - hiveServiceId = " + hiveServiceId);
		}

		RangerPolicy matchHdfsPolicy = searchHdfsPolicyByLocation(hdfsService.getId(), location);
		List<RangerPolicy> hivePolicies = new ArrayList<>();
		if (null != matchHdfsPolicy) {
			hivePolicies = searchHivePolicyByLocation(hiveServiceId, location);

			if (null != addPolicy) {
				RangerPolicy policyContains = policiesContains(hivePolicies, addPolicy.getId());
				if (null == policyContains) {
					hivePolicies.add(addPolicy);
				}
			}

			if (null != minusPolicy) {
				RangerPolicy containPolicy = policiesContains(hivePolicies, minusPolicy.getId());
				if (null != containPolicy) {
					hivePolicies.remove(containPolicy);
				}
			}

			if (hivePolicies.size() == 0) {
				svcStore.deletePolicy(matchHdfsPolicy.getId());
			} else {
				// update new hdfs-policy
				RangerPolicy genHdfsPolicy = generateHdfsPolicy(hivePolicies, location);
				if (genHdfsPolicy.getPolicyItems().size() == 0) {
					svcStore.deletePolicy(matchHdfsPolicy.getId());
				} else {
					// only modify policy item
					matchHdfsPolicy.getPolicyItems().clear();
					matchHdfsPolicy.getPolicyItems().addAll(genHdfsPolicy.getPolicyItems());
					svcStore.updatePolicy(matchHdfsPolicy);
				}
			}
		} else {
			RangerPolicy genHdfsPolicy = generateHdfsPolicy(hivePolicies, location);
			svcStore.createPolicy(genHdfsPolicy);
		}
	}

	public void setPolicyDesc(RangerPolicy policy, String key, String value) {
		String desc = policy.getDescription();
		Gson gson = new Gson();
		HashMap<String, String> mapDesc = null;
		mapDesc = gson.fromJson(desc, new TypeToken<HashMap<String, String>>() {
		}.getType());
		if (null == mapDesc) {
			mapDesc = new HashMap();
		}
		mapDesc.put(key, value);
		desc = gson.toJson(mapDesc);
		policy.setDescription(desc);
	}

	public String getPolicyDesc(RangerPolicy policy, String key) {
		String desc = policy.getDescription();

		Gson gson = new Gson();
		HashMap<String, String> mapDesc = gson.fromJson(desc, new TypeToken<HashMap<String, String>>() {
		}.getType());
		if (null == mapDesc) {
			return "";
		}
		return mapDesc.get(key);
	}

	public enum HiveAccessType {
		NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN
	}

	private RangerService getRelatedHdfsService(Long hiveServiceId) throws Exception {
		String hdfsServiceName = "";

		XXServiceConfigMap hdfsServiceConfig
				= daoManager.getXXServiceConfigMap().findByServiceAndConfigKey(hiveServiceId, "ranger.hdfs.service");
		if (null == hdfsServiceConfig) {
			throw new Exception("getRelatedHdfsService(" + hiveServiceId + ") is null");
		}
		hdfsServiceName = hdfsServiceConfig.getConfigvalue();
		RangerService hdfsServiceDef = getServiceByName(hdfsServiceName);
		if (null == hdfsServiceDef) {
			throw new Exception("service does not exist - name = " + hdfsServiceName);
		}

		return hdfsServiceDef;
	}

	private RangerPolicy alterHivePolicyResource(RangerPolicy hivePolicy, SynchronizeRequest syncRequest) throws Exception {
		Map<String, RangerPolicyResource> policyResources = new HashMap<>();
		RangerAccessResource newResource = new RangerAccessResourceImpl(syncRequest.getNewResource());
		Set<String> resourceNames = newResource.getKeys();

		// update policy database-name, table-name, columns-name
		if(! CollectionUtils.isEmpty(resourceNames)) {
			for(String resourceName : resourceNames) {
				RangerPolicyResource policyResource = new RangerPolicyResource(newResource.getValue(resourceName));
				policyResource.setIsRecursive(syncRequest.getIsRecursive());

				policyResources.put(resourceName, policyResource);
			}
		}
		hivePolicy.setResources(policyResources);

		return hivePolicy;
	}

	// auto create hive policy
	// hive policy name through the table name plus hdfs poicy name
	private RangerPolicy generateHivePolicy(String hiveServiceName, SynchronizeRequest syncRequest) {
		String dbName = syncRequest.getResource().get("database");
		String tabName = syncRequest.getResource().get("table");

		RangerPolicy policy = new RangerPolicy();
		RangerAccessResource resource = new RangerAccessResourceImpl(syncRequest.getResource());
		policy.setService(hiveServiceName);
		policy.setName(dbName + "-" + tabName + "-" + System.currentTimeMillis());
		policy.setIsAuditEnabled(syncRequest.getEnableAudit());
		policy.setCreatedBy(syncRequest.getGrantor());

		Map<String, String> mapDesc = new HashedMap();
		mapDesc.put(ServiceREST.POLICY_DESC_TABLE_TYPE, syncRequest.getTableType());
		mapDesc.put(ServiceREST.POLICY_DESC_TABLE_LOCATION, syncRequest.getLocation());
		Gson gson = new Gson();
		String desc = gson.toJson(mapDesc);
		policy.setDescription(desc);

		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		Set<String>                       resourceNames   = resource.getKeys();

		if(! CollectionUtils.isEmpty(resourceNames)) {
			for(String resourceName : resourceNames) {
				RangerPolicyResource policyResource = new RangerPolicyResource(resource.getValue(resourceName));
				policyResource.setIsRecursive(syncRequest.getIsRecursive());

				policyResources.put(resourceName, policyResource);
			}
		}
		policy.setResources(policyResources);

		RangerPolicyItem policyItem = new RangerPolicyItem();
		policyItem.getUsers().addAll(syncRequest.getUsers());
		policyItem.getGroups().addAll(syncRequest.getGroups());
		policyItem.getAccesses().add(new RangerPolicyItemAccess("ALL", Boolean.TRUE));
		policyItem.setDelegateAdmin(syncRequest.getDelegateAdmin());
		policy.getPolicyItems().add(policyItem);

		return policy;
	}

	// EXTERNAL_TABLE
	private RangerPolicy generateHdfsPolicy(List<RangerPolicy> hivePolicys, String location)
			throws Exception {
		if (hivePolicys.size() == 0) {
			return null;
		}

		String hiveServiceName = hivePolicys.get(0).getService();
		RangerService service = getServiceByName(hiveServiceName);
		if (null == service) {
			throw new Exception("service does not exist - name = " + hiveServiceName);
		}

		RangerService hdfsService = getRelatedHdfsService(service.getId());
		RangerPolicy newHdfsPolicy = new RangerPolicy();
		RangerPolicyResource policyResource = new RangerPolicyResource();
		Map<String, RangerPolicyResource> policyResources = new HashMap<>();
		URI uri = new URI(location);
		String hdfsPath = uri.getPath();
		policyResource.getValues().add(hdfsPath);
		policyResource.setIsRecursive(Boolean.TRUE);
		policyResources.put("path", policyResource);

		newHdfsPolicy.setResources(policyResources);
		newHdfsPolicy.setService(hdfsService.getName());
		newHdfsPolicy.setName(generateHdfsPolicyName(hivePolicys.get(0)));
		newHdfsPolicy.setIsAuditEnabled(true);
		newHdfsPolicy.setCreatedBy(hivePolicys.get(0).getCreatedBy());

		// access permissions
		List<RangerPolicyItem> hdfsPolicyItems = new ArrayList<>();
		RangerPolicyItemAccess readPolicyItemAccess = new RangerPolicyItemAccess("read", Boolean.TRUE);
		RangerPolicyItemAccess writePolicyItemAccess = new RangerPolicyItemAccess("write", Boolean.TRUE);
		RangerPolicyItemAccess executePolicyItemAccess = new RangerPolicyItemAccess("execute", Boolean.TRUE);
		for (int i = 0; i < hivePolicys.size(); i ++) {
			RangerPolicy hivePolicy = hivePolicys.get(i);
			String tableType = getPolicyDesc(hivePolicy, POLICY_DESC_TABLE_TYPE);

			for (RangerPolicyItem hivePolicyItem : hivePolicy.getPolicyItems()) {
				RangerPolicyItem hdfsPolicyItem = new RangerPolicyItem();

				// user and group
				hdfsPolicyItem.setUsers(hivePolicyItem.getUsers());
				hdfsPolicyItem.setGroups(hivePolicyItem.getGroups());
				hdfsPolicyItem.setDelegateAdmin(hivePolicyItem.getDelegateAdmin());

				// hdfs access type
				Map<String, RangerPolicyItemAccess> mapRangerPolicyItemAccess = new HashMap<>();
				List<RangerPolicyItemAccess> hdfsPolicyItemAccessList = new ArrayList<>();
				List<RangerPolicyItemAccess> hivePolicyItemAccessList = hivePolicyItem.getAccesses();
				for(RangerPolicyItemAccess hivePolicyItemAccess : hivePolicyItemAccessList) {
					if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.SELECT.name())
							|| tableType.equalsIgnoreCase(ServiceREST.EXTERNAL_TABLE_TYPE)) {
						// EXTERNAL TABLE, hdfs only read
						mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
						mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
					} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.UPDATE.name())
							|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALTER.name())
							|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.CREATE.name())
							|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.DROP.name())) {
						mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
						mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
					} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALL.name())) {
						mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
						mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
						mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
						break;
					}
				}
				for(Map.Entry<String, RangerPolicyItemAccess> accessEntry : mapRangerPolicyItemAccess.entrySet()) {
					hdfsPolicyItemAccessList.add(accessEntry.getValue());
				}
				hdfsPolicyItem.setAccesses(hdfsPolicyItemAccessList);
				hdfsPolicyItems.add(hdfsPolicyItem);
			}
			newHdfsPolicy.setPolicyItems(hdfsPolicyItems);
		}

		return newHdfsPolicy;
	}

	private RangerPolicy generateHdfsPolicy(RangerPolicy matchHivePolicy, String location)
			throws Exception {
		String hiveServiceName = matchHivePolicy.getService();
		RangerService service = getServiceByName(hiveServiceName);
		if (null == service) {
			throw new Exception("service does not exist - name = " + hiveServiceName);
		}

		// create new hdfs policy
		RangerPolicy newHdfsPolicy = new RangerPolicy();
		if (!location.isEmpty()) {
			RangerService hdfsService = getRelatedHdfsService(service.getId());
			RangerPolicyResource policyResource = new RangerPolicyResource();
			Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
			URI uri = new URI(location);
			String hdfsPath = uri.getPath();
			policyResource.getValues().add(hdfsPath);
			policyResource.setIsRecursive(Boolean.TRUE);
			policyResources.put("path", policyResource);

			newHdfsPolicy.setResources(policyResources);
			newHdfsPolicy.setService(hdfsService.getName());
		}

		newHdfsPolicy.setName(generateHdfsPolicyName(matchHivePolicy));
		newHdfsPolicy.setDescription("auto sync by " + matchHivePolicy.getName());
		newHdfsPolicy.setIsAuditEnabled(true);
		newHdfsPolicy.setCreatedBy(matchHivePolicy.getCreatedBy());

		// access permissions
		List<RangerPolicyItem> hdfsPolicyItems = new ArrayList<RangerPolicyItem>();
		RangerPolicyItemAccess readPolicyItemAccess = new RangerPolicyItemAccess("read", Boolean.TRUE);
		RangerPolicyItemAccess writePolicyItemAccess = new RangerPolicyItemAccess("write", Boolean.TRUE);
		RangerPolicyItemAccess executePolicyItemAccess = new RangerPolicyItemAccess("execute", Boolean.TRUE);
		for (RangerPolicyItem hivePolicyItem : matchHivePolicy.getPolicyItems()) {
			RangerPolicyItem hdfsPolicyItem = new RangerPolicyItem();

			// user and group
			hdfsPolicyItem.setUsers(hivePolicyItem.getUsers());
			hdfsPolicyItem.setGroups(hivePolicyItem.getGroups());
			hdfsPolicyItem.setDelegateAdmin(hivePolicyItem.getDelegateAdmin());

			// hdfs access type
			Map<String, RangerPolicyItemAccess> mapRangerPolicyItemAccess = new HashMap<>();
			List<RangerPolicyItemAccess> hdfsPolicyItemAccessList = new ArrayList<RangerPolicyItemAccess>();
			List<RangerPolicyItemAccess> hivePolicyItemAccessList = hivePolicyItem.getAccesses();
			for(RangerPolicyItemAccess hivePolicyItemAccess : hivePolicyItemAccessList) {
				if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.SELECT.name())) {
					mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
					mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
				} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.UPDATE.name())
						|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALTER.name())
						|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.CREATE.name())
						|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.DROP.name())) {
					mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
					mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
				} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALL.name())) {
					mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
					mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
					mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
					break;
				}
			}
			for(Map.Entry<String, RangerPolicyItemAccess> accessEntry : mapRangerPolicyItemAccess.entrySet()) {
				hdfsPolicyItemAccessList.add(accessEntry.getValue());
			}
			hdfsPolicyItem.setAccesses(hdfsPolicyItemAccessList);
			hdfsPolicyItems.add(hdfsPolicyItem);
		}
		newHdfsPolicy.setPolicyItems(hdfsPolicyItems);

		return newHdfsPolicy;
	}

	private RangerPolicyItem convertHivePolicy2HdfsPolicyItem(RangerPolicyItem hivePolicyItem) {
		RangerPolicyItem hdfsPolicyItem = new RangerPolicyItem();

		// user and group
		hdfsPolicyItem.setUsers(hivePolicyItem.getUsers());
		hdfsPolicyItem.setDelegateAdmin(hivePolicyItem.getDelegateAdmin());

		// access permissions
		RangerPolicyItemAccess readPolicyItemAccess = new RangerPolicyItemAccess("read", Boolean.TRUE);
		RangerPolicyItemAccess writePolicyItemAccess = new RangerPolicyItemAccess("write", Boolean.TRUE);
		RangerPolicyItemAccess executePolicyItemAccess = new RangerPolicyItemAccess("execute", Boolean.TRUE);

		// hdfs access type
		Map<String, RangerPolicyItemAccess> mapRangerPolicyItemAccess = new HashMap<>();
		List<RangerPolicyItemAccess> hdfsPolicyItemAccessList = new ArrayList<RangerPolicyItemAccess>();
		List<RangerPolicyItemAccess> hivePolicyItemAccessList = hivePolicyItem.getAccesses();
		for(RangerPolicyItemAccess hivePolicyItemAccess : hivePolicyItemAccessList) {
			if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.SELECT.name())) {
				mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
				mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
			} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.UPDATE.name())
					|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALTER.name())
					|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.CREATE.name())
					|| StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.DROP.name())) {
				mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
				mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
			} else if (StringUtils.equalsIgnoreCase(hivePolicyItemAccess.getType(), HiveAccessType.ALL.name())) {
				mapRangerPolicyItemAccess.put("read", readPolicyItemAccess);
				mapRangerPolicyItemAccess.put("write", writePolicyItemAccess);
				mapRangerPolicyItemAccess.put("execute", executePolicyItemAccess);
				break;
			}
		}
		for(Map.Entry<String, RangerPolicyItemAccess> accessEntry : mapRangerPolicyItemAccess.entrySet()) {
			hdfsPolicyItemAccessList.add(accessEntry.getValue());
		}
		hdfsPolicyItem.setAccesses(hdfsPolicyItemAccessList);

		return hdfsPolicyItem;
	}

	private void hdfsPolicyAddHiveUserPermissiom(RangerPolicy hdfsPolicy, RangerPolicy matchHivePolicy)
			throws Exception {
		String hiveServiceName = matchHivePolicy.getService();
		RangerService service = getServiceByName(hiveServiceName);
		if (null == service) {
			throw new Exception("service does not exist - name = " + hiveServiceName);
		}

		// access permissions
		List<RangerPolicyItem> hdfsPolicyItems = hdfsPolicy.getPolicyItems();
		for (RangerPolicyItem hivePolicyItem : matchHivePolicy.getPolicyItems()) {
			RangerPolicyItem hdfsPolicyItem = convertHivePolicy2HdfsPolicyItem(hivePolicyItem);
			hdfsPolicyItems.add(hdfsPolicyItem);
		}
		hdfsPolicy.setPolicyItems(hdfsPolicyItems);
	}

	// HDFS-Policy minus Hive-Policy all user permission
	private boolean hdfsPolicyMinusHiveUserPermissiom(RangerPolicy hdfsPolicy, RangerPolicy matchHivePolicy)
			throws Exception {

		String hiveServiceName = matchHivePolicy.getService();
		RangerService service = getServiceByName(hiveServiceName);
		if (null == service) {
			throw new Exception("service does not exist - name = " + hiveServiceName);
		}

		// conert hive to hdfs policy item
		List<RangerPolicyItem> convert2HdfsPolicyItems = new ArrayList<>();
		for (RangerPolicyItem hivePolicyItem : matchHivePolicy.getPolicyItems()) {
			RangerPolicyItem hdfsPolicyItem = convertHivePolicy2HdfsPolicyItem(hivePolicyItem);
			convert2HdfsPolicyItems.add(hdfsPolicyItem);
		}

		// minus hive user permissions
		for (RangerPolicyItem hdfsPolicyItem : hdfsPolicy.getPolicyItems()) {
			for (RangerPolicyItem convertPolicyItem : convert2HdfsPolicyItems) {
				if (policyItemEquals(hdfsPolicyItem, convertPolicyItem)) {
					hdfsPolicy.getPolicyItems().remove(hdfsPolicyItem);
				}
			}
		}

		if (hdfsPolicy.getPolicyItems().size() == 0) {
			return true;
		}

		return false;
	}

	private boolean policyItemEquals(RangerPolicyItem hdfsPolicyItem1, RangerPolicyItem hdfsPolicyItem2) {
		if (false == hdfsPolicyItem1.getUsers().containsAll(hdfsPolicyItem2.getUsers())) {
			return false;
		}

		if (false == hdfsPolicyItem1.getGroups().containsAll(hdfsPolicyItem2.getGroups())) {
			return false;
		}

		if (false == hdfsPolicyItem1.getAccesses().containsAll(hdfsPolicyItem2.getAccesses())) {
			return false;
		}

		return true;
	}


	private String generateHdfsPolicyName(RangerPolicy hivePolicy) {
		// service name and policy id will not change
		return "sync-" + hivePolicy.getName();
	}

	@POST
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createPolicy(" + policy + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createPolicy(policyName=" + policy.getName() + ")");
		}
		
		try {
			// this needs to happen before validator is called
			// set name of policy if unspecified
			if (StringUtils.isBlank(policy.getName())) { // use of isBlank over isEmpty is deliberate as a blank string does not strike us as a particularly useful policy name!
				String guid = policy.getGuid();
				if (StringUtils.isBlank(guid)) { // use of isBlank is deliberate. External parties could send the guid in, perhaps to sync between dev/test/prod instances?
					guid = guidUtil.genGUID();
					policy.setGuid(guid);
					if (LOG.isDebugEnabled()) {
						LOG.debug("No GUID supplied on the policy!  Ok, setting GUID to [" + guid + "].");
					}
				}
				String name = policy.getService() + "-" + guid;
				policy.setName(name);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Policy did not have its name set!  Ok, setting name to [" + name + "]");
				}
			}
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(policy, Action.CREATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			// update hive description
			XXService xxService = daoManager.getXXService().findByName(policy.getService());
			RangerService rangerService = svcService.getPopulatedViewObject(xxService);
			if (null == rangerService) {
				LOG.error("servicedef does not exist - name=" + policy.getService());
			} else {
				if (rangerService.getType().equalsIgnoreCase("hive")) {
					updateHivePolicyDescByTableName(rangerService.getId(), policy);
				}
			}

			ret = svcStore.createPolicy(policy);

			// last synchronize hdfs policy
			if (null == rangerService) {
				LOG.error("servicedef does not exist - name=" + policy.getService());
			} else {
				if (rangerService.getType().equalsIgnoreCase("hive")) {
					String location = getPolicyDesc(policy, POLICY_DESC_TABLE_LOCATION);
					adjustHdfsPolicyByLocation(rangerService.getId(), location, null, null);
				}
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createPolicy(" + policy + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updatePolicy(" + policy + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updatePolicy(policyId=" + policy.getId() + ")");
		}

		try {
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(policy, Action.UPDATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			ret = svcStore.updatePolicy(policy);

			// last synchronize hdfs policy
			XXService xxService = daoManager.getXXService().findByName(policy.getService());
			RangerService rangerService = svcService.getPopulatedViewObject(xxService);
			if (null == rangerService) {
				LOG.error("servicedef does not exist - name=" + policy.getService());
			} else {
				if (rangerService.getType().equalsIgnoreCase("hive")) {
					String location = getPolicyDesc(policy, POLICY_DESC_TABLE_LOCATION);
					adjustHdfsPolicyByLocation(rangerService.getId(), location, null, null);
				}
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updatePolicy(" + policy + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updatePolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public void deletePolicy(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deletePolicy(" + id + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deletePolicy(policyId=" + id + ")");
		}

		try {
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(id, Action.DELETE);

			RangerPolicy policy = svcStore.getPolicy(id);

			ensureAdminAccess(policy.getService(), policy.getResources());

			svcStore.deletePolicy(id);

			// last synchronize hdfs policy
			XXService xxService = daoManager.getXXService().findByName(policy.getService());
			RangerService rangerService = svcService.getPopulatedViewObject(xxService);
			if (null == rangerService) {
				LOG.error("servicedef does not exist - name=" + policy.getService());
			} else {
				if (rangerService.getType().equalsIgnoreCase("hive")) {
					String location = getPolicyDesc(policy, POLICY_DESC_TABLE_LOCATION);
					adjustHdfsPolicyByLocation(rangerService.getId(), location, null, null);
				}
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deletePolicy(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deletePolicy(" + id + ")");
		}
	}

	@GET
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicy(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicy(" + id + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicy(policyId=" + id + ")");
		}

		try {
			ret = svcStore.getPolicy(id);

			if(ret != null) {
				ensureAdminAccess(ret.getService(), ret.getResources());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicy(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getPolicies(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies()");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;
		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
		}
		if(isAdminUserWithNoFilterParams(filter)) {
			ret = svcStore.getPaginatedPolicies(filter);
		}
		 else {
				// get all policies from the store; pick the page to return after applying filter
				int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
				int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

				if(filter != null) {
					filter.setStartIndex(0);
					filter.setMaxRows(Integer.MAX_VALUE);
				}

				List<RangerPolicy> policies = svcStore.getPolicies(filter);

				if(filter != null) {
					filter.setStartIndex(savedStartIndex);
					filter.setMaxRows(savedMaxRows);
				}

				policies = applyAdminAccessFilter(policies);

				ret = toRangerPolicyList(policies, filter);
			}

		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicies(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	public List<RangerPolicy> getPolicies(SearchFilter filter) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies(filter)");
		}

		List<RangerPolicy> ret  = null;
		RangerPerfTracer   perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
		}

		try {
			ret = svcStore.getPolicies(filter);

			ret = applyAdminAccessFilter(ret);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicies(filter): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@GET
	@Path("/policies/count")
	@Produces({ "application/json", "application/xml" })
	public Long countPolicies( @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.countPolicies():");
		}

		Long             ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countPolicies()");
		}

		try {
			List<RangerPolicy> policies = getPolicies(request).getPolicies();

			policies = applyAdminAccessFilter(policies);
			
			ret = new Long(policies == null ? 0 : policies.size());
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("countPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countPolicies(): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/policies/service/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getServicePolicies(@PathParam("id") Long serviceId,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceId + ")");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;
		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceId=" + serviceId + ")");
			}
			if(isAdminUserWithNoFilterParams(filter)) {
				ret = svcStore.getPaginatedServicePolicies(serviceId, filter);
			} else {
				// get all policies from the store; pick the page to return after applying filter
				int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
				int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

				if(filter != null) {
					filter.setStartIndex(0);
					filter.setMaxRows(Integer.MAX_VALUE);
				}

				List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceId, filter);

				if(filter != null) {
					filter.setStartIndex(savedStartIndex);
					filter.setMaxRows(savedMaxRows);
				}

				servicePolicies = applyAdminAccessFilter(servicePolicies);

				ret = toRangerPolicyList(servicePolicies, filter);
			}

		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServicePolicies(" + serviceId + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceId + "): count="
					+ ret.getListSize());
		}
		return ret;
	}

	@GET
	@Path("/policies/service/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getServicePoliciesByName(@PathParam("name") String serviceName,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceName + ")");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceName=" + serviceName + ")");
		}

		if(isAdminUserWithNoFilterParams(filter)) {
			ret = svcStore.getPaginatedServicePolicies(serviceName, filter);
		} else {
			// get all policies from the store; pick the page to return after applying filter
			int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
			int savedMaxRows = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

			if (filter != null) {
				filter.setStartIndex(0);
				filter.setMaxRows(Integer.MAX_VALUE);
			}

			List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceName, filter);

			if (filter != null) {
				filter.setStartIndex(savedStartIndex);
				filter.setMaxRows(savedMaxRows);
			}


			servicePolicies = applyAdminAccessFilter(servicePolicies);
			ret = toRangerPolicyList(servicePolicies, filter);
		}


		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServicePolicies(" + serviceName + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (ret == null) {
			LOG.info("No Policies found for given service name: " + serviceName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceName + "): count="
					+ ret != null ? ret.getListSize() : ret);
		}

		return ret;
	}

	@GET
	@Path("/policies/download/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public ServicePolicies getServicePoliciesIfUpdated(@PathParam("serviceName") String serviceName, @QueryParam("lastKnownVersion") Long lastKnownVersion, @QueryParam("pluginId") String pluginId, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret      = null;
		int             httpCode = HttpServletResponse.SC_OK;
		String          logMsg   = null;
		RangerPerfTracer perf    = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePoliciesIfUpdated(serviceName=" + serviceName + ")");
		}

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
			if(lastKnownVersion == null) {
				lastKnownVersion = new Long(-1);
			}
			
			try {
				ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);
	
				if(ret == null) {
					httpCode = HttpServletResponse.SC_NOT_MODIFIED;
					logMsg   = "No change since last update";
				} else {
					httpCode = HttpServletResponse.SC_OK;
					logMsg   = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
				}
			} catch(Throwable excp) {
				LOG.error("getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ") failed", excp);
	
				httpCode = HttpServletResponse.SC_BAD_REQUEST;
				logMsg   = excp.getMessage();
			} finally {
				createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, ret, httpCode, request);

				RangerPerfTracer.log(perf);
			}
	
			if(httpCode != HttpServletResponse.SC_OK) {
				boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;
				throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
			}
		}
 
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}
   
		return ret;
	}

	@GET
	@Path("/policies/download/ini/{serviceName}")
	@Produces({ "text/html" })
	public String getServicePoliciesIniIfUpdated(@PathParam("serviceName") String serviceName, @QueryParam("lastKnownVersion") Long lastKnownVersion, @QueryParam("pluginId") String pluginId, @Context HttpServletRequest request) throws Exception {
		String strIni = "";
		try {
			ServicePolicies servicePolicies = getServicePoliciesIfUpdated(serviceName, lastKnownVersion, pluginId, request);
			RangerRestUtil rangerRestUtil = new RangerRestUtil();

			strIni = rangerRestUtil.toSentryProviderIni(serviceName, servicePolicies);
		} catch(Exception excp) {
			LOG.error("getServicePoliciesIni(" + serviceName + ", " + lastKnownVersion + ") failed", excp);
		}
		return strIni;
	}

	private void createPolicyDownloadAudit(String serviceName, Long lastKnownVersion, String pluginId, ServicePolicies policies, int httpRespCode, HttpServletRequest request) {
		try {
			String ipAddress = request.getHeader("X-FORWARDED-FOR");

			if (ipAddress == null) {  
				ipAddress = request.getRemoteAddr();
			}

			XXPolicyExportAudit policyExportAudit = new XXPolicyExportAudit();

			policyExportAudit.setRepositoryName(serviceName);
			policyExportAudit.setAgentId(pluginId);
			policyExportAudit.setClientIP(ipAddress);
			policyExportAudit.setRequestedEpoch(lastKnownVersion);
			policyExportAudit.setHttpRetCode(httpRespCode);

			assetMgr.createPolicyAudit(policyExportAudit);
		} catch(Exception excp) {
			LOG.error("error while creating policy download audit", excp);
		}
	}

	private RangerPolicy getExactMatchPolicyForResource(String serviceName, RangerAccessResource resource) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getExactMatchPolicyForResource(" + resource + ")");
		}

		RangerPolicy       ret          = null;
		RangerPolicyEngine policyEngine = getPolicyEngine(serviceName);
		List<RangerPolicy> policies     = policyEngine != null ? policyEngine.getExactMatchPolicies(resource) : null;

		if(CollectionUtils.isNotEmpty(policies)) {
			// at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
			ret = svcStore.getPolicy(policies.get(0).getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getExactMatchPolicyForResource(" + resource + "): " + ret);
		}

		return ret;
	}

	private boolean compactPolicy(RangerPolicy policy) {
		boolean ret = false;

		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

		int numOfItems = policyItems.size();
		
		for(int i = 0; i < numOfItems; i++) {
			RangerPolicyItem policyItem = policyItems.get(i);
			
			// remove the policy item if 1) there are no users and groups OR 2) if there are no accessTypes and not a delegate-admin
			if((CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) ||
			   (CollectionUtils.isEmpty(policyItem.getAccesses()) && !policyItem.getDelegateAdmin())) {
				policyItems.remove(i);
				numOfItems--;
				i--;

				ret = true;
			}
		}

		return ret;
	}

	private RangerPolicyItem getPolicyItemForUser(RangerPolicy policy, String userName) {
		RangerPolicyItem ret = null;

		for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
			if(policyItem.getUsers().size() != 1) {
				continue;
			}

			if(policyItem.getUsers().contains(userName)) {
				ret = policyItem;
				break;
			}
		}

		return ret;
	}

	private RangerPolicyItem getPolicyItemForGroup(RangerPolicy policy, String groupName) {
		RangerPolicyItem ret = null;

		for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
			if(policyItem.getGroups().size() != 1) {
				continue;
			}

			if(policyItem.getGroups().contains(groupName)) {
				ret = policyItem;
				break;
			}
		}

		return ret;
	}

	private boolean addAccesses(RangerPolicyItem policyItem, Set<String> accessTypes) {
		boolean ret = false;

		for(String accessType : accessTypes) {
			RangerPolicyItemAccess policyItemAccess = null;

			for(RangerPolicyItemAccess itemAccess : policyItem.getAccesses()) {
				if(StringUtils.equals(itemAccess.getType(), accessType)) {
					policyItemAccess = itemAccess;
					break;
				}
			}

			if(policyItemAccess != null) {
				if(!policyItemAccess.getIsAllowed()) {
					policyItemAccess.setIsAllowed(Boolean.TRUE);
					ret = true;
				}
			} else {
				policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
				ret = true;
			}
		}

		return ret;
	}

	private boolean removeAccesses(RangerPolicyItem policyItem, Set<String> accessTypes) {
		boolean ret = false;

		for(String accessType : accessTypes) {
			int numOfItems = policyItem.getAccesses().size();

			for(int i = 0; i < numOfItems; i++) {
				RangerPolicyItemAccess itemAccess = policyItem.getAccesses().get(i);
				
				if(StringUtils.equals(itemAccess.getType(), accessType)) {
					policyItem.getAccesses().remove(i);
					numOfItems--;
					i--;

					ret = true;
				}
			}
		}

		return ret;
	}

	private boolean removeUsersAndGroupsFromPolicy(RangerPolicy policy, Set<String> users, Set<String> groups) {
		boolean policyUpdated = false;

		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

		int numOfItems = policyItems.size();

		for(int i = 0; i < numOfItems; i++) {
			RangerPolicyItem policyItem = policyItems.get(i);

			if(CollectionUtils.containsAny(policyItem.getUsers(), users)) {
				policyItem.getUsers().removeAll(users);

				policyUpdated = true;
			}

			if(CollectionUtils.containsAny(policyItem.getGroups(), groups)) {
				policyItem.getGroups().removeAll(groups);

				policyUpdated = true;
			}

			if(CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
				policyItems.remove(i);
				numOfItems--;
				i--;

				policyUpdated = true;
			}
		}

		return policyUpdated;
	}

	@GET
	@Path("/policies/eventTime")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FROM_EVENT_TIME + "\")")
	public RangerPolicy getPolicyFromEventTime(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicyFromEventTime()");
		}

		String eventTimeStr = request.getParameter("eventTime");
		String policyIdStr = request.getParameter("policyId");

		if (StringUtils.isEmpty(eventTimeStr) || StringUtils.isEmpty(policyIdStr)) {
			throw restErrorUtil.createRESTException("EventTime or policyId cannot be null or empty string.",
					MessageEnums.INVALID_INPUT_DATA);
		}

		Long policyId = Long.parseLong(policyIdStr);

		RangerPolicy policy=null;
		try {
			policy = svcStore.getPolicyFromEventTime(eventTimeStr, policyId);
			if(policy != null) {
				ensureAdminAccess(policy.getService(), policy.getResources());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicy(" + policyId + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		}

		if(policy == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicy(" + policyId + "): " + policy);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyFromEventTime()");
		}

		return policy;
	}

	@GET
	@Path("/policy/{policyId}/versionList")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_VERSION_LIST + "\")")
	public VXString getPolicyVersionList(@PathParam("policyId") Long policyId) {
		return svcStore.getPolicyVersionList(policyId);
	}

	@GET
	@Path("/policy/{policyId}/version/{versionNo}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FOR_VERSION_NO + "\")")
	public RangerPolicy getPolicyForVersionNumber(@PathParam("policyId") Long policyId,
			@PathParam("versionNo") int versionNo) {
		return svcStore.getPolicyForVersionNumber(policyId, versionNo);
	}

	private List<RangerPolicy> applyAdminAccessFilter(List<RangerPolicy> policies) {
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();
		RangerPerfTracer  perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.applyAdminAccessFilter(policyCount=" + (policies == null ? 0 : policies.size()) + ")");
		}

		if (CollectionUtils.isNotEmpty(policies)) {
			boolean     isAdmin    = bizUtil.isAdmin();
			boolean     isKeyAdmin = bizUtil.isKeyAdmin();
			String      userName   = bizUtil.getCurrentUserLoginId();
			Set<String> userGroups = null;

			Map<String, List<RangerPolicy>> servicePoliciesMap = new HashMap<String, List<RangerPolicy>>();

			for (int i = 0; i < policies.size(); i++) {
				RangerPolicy       policy      = policies.get(i);
				String             serviceName = policy.getService();
				List<RangerPolicy> policyList  = servicePoliciesMap.get(serviceName);

				if (policyList == null) {
					policyList = new ArrayList<RangerPolicy>();

					servicePoliciesMap.put(serviceName, policyList);
				}

				policyList.add(policy);
			}

			for (Map.Entry<String, List<RangerPolicy>> entry : servicePoliciesMap.entrySet()) {
				String             serviceName  = entry.getKey();
				List<RangerPolicy> listToFilter = entry.getValue();

				if (CollectionUtils.isNotEmpty(listToFilter)) {
					if (isAdmin || isKeyAdmin) {
						XXService xService     = daoManager.getXXService().findByName(serviceName);
						Long      serviceDefId = xService.getType();
						boolean   isKmsService = serviceDefId.equals(EmbeddedServiceDefsUtil.instance().getKmsServiceDefId());

						if (isAdmin) {
							if (!isKmsService) {
								ret.addAll(listToFilter);
							}
						} else { // isKeyAdmin
							if (isKmsService) {
								ret.addAll(listToFilter);
							}
						}

						continue;
					}

					RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(serviceName);

					if (policyEngine != null) {
						if(userGroups == null) {
							userGroups = daoManager.getXXGroupUser().findGroupNamesByUserName(userName);
						}

						for (RangerPolicy policy : listToFilter) {
							if (policyEngine.isAccessAllowed(policy.getResources(), userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS)) {
								ret.add(policy);
							}
						}
					}

				}
			}
		}

		RangerPerfTracer.log(perf);

		return ret;
	}

	void ensureAdminAccess(String serviceName, Map<String, RangerPolicyResource> resources) {
		boolean isAdmin = bizUtil.isAdmin();
		boolean isKeyAdmin = bizUtil.isKeyAdmin();

		XXService xService = daoManager.getXXService().findByName(serviceName);
		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

		if(!isAdmin && !isKeyAdmin) {
			String             userName     = bizUtil.getCurrentUserLoginId();
			Set<String>        userGroups   = userMgr.getGroupsForUser(userName);

			boolean isAllowed = hasAdminAccess(serviceName, userName, userGroups, resources);

			if(!isAllowed) {
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED,
						"User '" + userName + "' does not have delegated-admin privilege on given resources", true);
			}
		} else if (isAdmin) {
			if (xServiceDef.getImplclassname().equals(EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
				throw restErrorUtil.createRESTException(
						"KMS Policies/Services/Service-Defs are not accessible for logged in user.",
						MessageEnums.OPER_NO_PERMISSION);
			}
		} else if (isKeyAdmin) {
			if (!xServiceDef.getImplclassname().equals(EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
				throw restErrorUtil.createRESTException(
						"Only KMS Policies/Services/Service-Defs are accessible for logged in user.",
						MessageEnums.OPER_NO_PERMISSION);
			}
		}
	}

	private boolean hasAdminAccess(String serviceName, String userName, Set<String> userGroups, Map<String, RangerPolicyResource> resources) {
		boolean isAllowed = false;

		RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(serviceName);

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(resources, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

	private boolean hasAdminAccess(String serviceName, String userName, Set<String> userGroups, RangerAccessResource resource) {
		boolean isAllowed = false;

		RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(serviceName);

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(resource, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

	private RangerPolicyEngine getDelegatedAdminPolicyEngine(String serviceName) {
		if(RangerPolicyEngineCache.getInstance().getPolicyEngineOptions() == null) {
			RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();

			String propertyPrefix = "ranger.admin";

			options.evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
			options.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", false);
			options.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
			options.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
			options.evaluateDelegateAdminOnly = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.evaluate.delegateadmin.only", true);

			RangerPolicyEngineCache.getInstance().setPolicyEngineOptions(options);;
		}

		RangerPolicyEngine ret = RangerPolicyEngineCache.getInstance().getPolicyEngine(serviceName, svcStore);

		return ret;
	}

	private RangerPolicyEngine getPolicyEngine(String serviceName) throws Exception {
		RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();

		String propertyPrefix = "ranger.admin";

		options.evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
		options.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", false);
		options.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		options.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		options.evaluateDelegateAdminOnly = false;

		ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, -1L);

		RangerPolicyEngine ret = new RangerPolicyEngineImpl(policies, options);

		return ret;
	}

	boolean isAdminUserWithNoFilterParams(SearchFilter filter) {
		return (filter == null || MapUtils.isEmpty(filter.getParams())) &&
			   (bizUtil.isAdmin() || bizUtil.isKeyAdmin());
	}

	private RangerPolicyList toRangerPolicyList(List<RangerPolicy> policyList, SearchFilter filter) {
		RangerPolicyList ret = new RangerPolicyList();

		if(CollectionUtils.isNotEmpty(policyList)) {
			int    totalCount = policyList.size();
			int    startIndex = filter == null ? 0 : filter.getStartIndex();
			int    pageSize   = filter == null ? totalCount : filter.getMaxRows();
			int    toIndex    = Math.min(startIndex + pageSize, totalCount);
			String sortType   = filter == null ? null : filter.getSortType();
			String sortBy     = filter == null ? null : filter.getSortBy();

			List<RangerPolicy> retList = new ArrayList<RangerPolicy>();
			for(int i = startIndex; i < toIndex; i++) {
				retList.add(policyList.get(i));
			}

			ret.setPolicies(retList);
			ret.setPageSize(pageSize);
			ret.setResultSize(retList.size());
			ret.setStartIndex(startIndex);
			ret.setTotalCount(totalCount);
			ret.setSortBy(sortBy);
			ret.setSortType(sortType);
		}

		return ret;
	}
}
