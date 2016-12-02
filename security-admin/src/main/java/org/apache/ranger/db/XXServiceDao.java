/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.db;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.NoResultException;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXService;

/**
 */

public class XXServiceDao extends BaseDao<XXService> {
	
	static final Logger logger = Logger.getLogger(XXServiceDao.class);
	
	/**
	 * Default Constructor
	 */
	public XXServiceDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}

	public XXService findByName(String name) {
		if (name == null) {
			return null;
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXService.findByName", tClass)
					.setParameter("name", name).getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public Long getMaxIdOfXXService() {
		try {
			return (Long) getEntityManager().createNamedQuery("XXService.getMaxIdOfXXService").getSingleResult();
		} catch (NoResultException e) {
			return null;
		}
	}

	public List<XXService> findByServiceDefId(Long serviceDefId) {
		if (serviceDefId == null) {
			return new ArrayList<XXService>();
		}
		try {
			return getEntityManager().createNamedQuery("XXService.findByServiceDefId", tClass)
					.setParameter("serviceDefId", serviceDefId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXService>();
		}
	}

	public void updateSequence() {
		Long maxId = getMaxIdOfXXService();

		if(maxId == null) {
			return;
		}

		updateSequence("X_SERVICE_SEQ", maxId + 1);
	}
	
	// add by hzlimin2
	public void updatePolicyVersion() {
		
		String query = "update x_service set policy_version = "
				+ "(case when policy_version is NULL then 1 when policy_version is not NULL then policy_version + 1 end)";
		int count=getEntityManager().createNativeQuery(query).executeUpdate();
		if(count>0){
			logger.warn(count + " records updated in table x_service with column policy_version");
		}
	}
}