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

 
define(function(require){
	'use strict';	

	var VXPortalUserBase	= require('model_bases/VXPortalUserBase');
	var XAEnums			= require('utils/XAEnums');
	var XAUtils			= require('utils/XAUtils');
	var localization		= require('utils/XALangSupport');
	
	var VXPortalUser = VXPortalUserBase.extend(
	/** @lends VXPortalUser.prototype */
	{
		/**
		 * @function schema
		 * This method is meant to be used by UI,
		 * by default we will remove the unrequired attributes from serverSchema
		 */

		schema : function(){
			var attrs = _.omit(this.serverSchema, 'id', 'createDate', 'updateDate', "version",
					"createDate", "updateDate", "displayOption",
					"permList", "forUserId", "status", "priGrpId",
					 "updatedBy","isSystem");

			_.each(attrs, function(o){
				o.type = 'Hidden';
			});

			// Overwrite your schema definition here
			return _.extend(attrs,{
				firstName : {
					type		: 'Text',
					title		: localization.tt("lbl.firstName")+' *',
					validators  : ['required',{type:'regexp',regexp:/^[a-zA-Z][a-zA-Z0-9\s_-]*[a-zA-Z0-9]+$/,message :'First name should start with alphabets & can have underscore, hyphen, space.'}],
					editorAttrs : { 'placeholder' : localization.tt("lbl.firstName")}
					
				},
				lastName : {
					type		: 'Text',
					title		: localization.tt("lbl.lastName"),
					validators  : ['required',{type:'regexp',regexp:/^[a-zA-Z][a-zA-Z0-9\s_-]*[a-zA-Z0-9]+$/,message :'Last name should start with alphabets & can have underscore, hyphen, space.'}],
					editorAttrs : { 'placeholder' : localization.tt("lbl.lastName")}
				},
				emailAddress : {
					type		: 'Text',
					title		: localization.tt("lbl.emailAddress"),
					validators  : ['email'],
					editorAttrs : { 'placeholder' : localization.tt("lbl.emailAddress")}//'disabled' : true}
					
				},
				oldPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.oldPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.oldPassword"),'onpaste':'return false;','oncopy':'return false;','autocomplete':'off'}
					
				},
				newPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.newPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.newPassword"),'onpaste':'return false;','oncopy':'return false;','autocomplete':'off'}
					
				},
				reEnterPassword : {
					type		: 'Password',
					title		: localization.tt("lbl.reEnterPassword")+' *',
				//	validators  : ['required'],
					fieldAttrs : {style : 'display:none;'},
					editorAttrs : { 'placeholder' : localization.tt("lbl.reEnterPassword"),'onpaste':'return false;','oncopy':'return false;','autocomplete':'off'}
					
				},
				userRoleList : {
					type : 'Select',
					options : function(callback, editor){
						var userTypes = _.filter(XAEnums.UserRoles,function(m){return m.label != 'Unknown'});
						var nvPairs = XAUtils.enumToSelectPairs(userTypes);
						callback(nvPairs);
					},
					title : localization.tt('lbl.selectRole')+' *',
					editorAttrs : {disabled:'disabled'},
				}
			});
		},
		/** This models toString() */
		toString : function(){
			return /*this.get('name')*/;
		}

	}, {
		// static class members
	});

    return VXPortalUser;
	
});


