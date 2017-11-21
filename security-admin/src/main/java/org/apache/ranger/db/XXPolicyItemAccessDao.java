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

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXPolicyItemAccess;

public class XXPolicyItemAccessDao extends BaseDao<XXPolicyItemAccess> {

	public XXPolicyItemAccessDao(RangerDaoManagerBase daoManager) {
		super(daoManager);
	}
	
	public List<XXPolicyItemAccess> findByPolicyItemId(Long polItemId) {
		if(polItemId == null) {
			return new ArrayList<XXPolicyItemAccess>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemAccess.findByPolicyItemId", tClass)
					.setParameter("polItemId", polItemId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemAccess>();
		}
	}

	public List<XXPolicyItemAccess> findByPolicyId(Long policyId) {
		if(policyId == null) {
			return new ArrayList<XXPolicyItemAccess>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemAccess.findByPolicyId", tClass)
					.setParameter("policyId", policyId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemAccess>();
		}
	}

	public List<XXPolicyItemAccess> findByServiceId(Long serviceId) {
		if(serviceId == null) {
			return new ArrayList<XXPolicyItemAccess>();
		}
		try {
			return getEntityManager()
					.createNamedQuery("XXPolicyItemAccess.findByServiceId", tClass)
					.setParameter("serviceId", serviceId).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemAccess>();
		}
	}

	public List<XXPolicyItemAccess> findByType(Long type) {
		if (type == null) {
			return new ArrayList<XXPolicyItemAccess>();
		}
		try {
			return getEntityManager().createNamedQuery("XXPolicyItemAccess.findByType", tClass)
					.setParameter("type", type).getResultList();
		} catch (NoResultException e) {
			return new ArrayList<XXPolicyItemAccess>();
		}
	}

	public void deleteByPolicyItemIds(List<Long> policyItemIds) {
		getEntityManager()
			.createNamedQuery("XXPolicyItemAccess.deleteByPolicyItemIds")
			.setParameter("policyItemIds", policyItemIds)
			.executeUpdate();
	}
}
