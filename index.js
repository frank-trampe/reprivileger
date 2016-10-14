// Copyright 2012-2016 by Frank Trampe.
// All rights reserved except as granted by an accompanying license.

'use strict';

var path = require('path');
var compress = require('compression');
var cors = require('cors');
var bodyParser = require('body-parser');
// var Promise = require('promise');
var express = require('express');
var ReadWriteLock = require('rwlock');
var app = express();

var security_template = {
	// This is set in order to facilitate access to the app data.
	// 'app': app,
	// This module currently requires all services on which security is enforced to use the same primary key name.
	// id_name: '_id',
	// data_schema is an object. Each of its subobjects represents a class or data type and has a subobject named fields.
	// Each subobject of fields represents a field of the class or data type.
	// Each subobject of the field represents a rule or a property of the field.
	// These include
	//	type, a string representation of the JavaScript type of the field, such as 'string', to be enforced
	// 	validation, a string of feathers-validator rules to be enforced
	// 	is_primary_key, a number set to 0 or to 1 with obvious meaning
	// 	is_user_writable, a number, which must be 1 if the user is to be able to write a record with that value specified
	// 	target_class, a string naming the class (specified in data_schema) to which the field points
	// 	target_authority, a number specifying the level of access that the target_user must have to the referenced record (using the field value as the index value, target_class from the schema as the service/table/class name, and the id_name from the security service as the index name) in order to create a record holding such reference.
	// 	recursive_reference_check, a numeric flag specifying whether to check the validity and non-loopiness of the reference chain from this field (within the same class, using the security service id_name as the primary key) before allowing creation/updates
	// .
	// Example:
	// 	data_schema: {
	//		'ships': {
	//			_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
	//			owner_id: {type: 'string', validation: 'max:255|alpha_dash', is_user_writable: 1, target_class: 'partners', target_authority: 2},
	// 			created_at: {type: 'date', validation: '', is_user_writable: 0},
	// 			created_by: {type: 'date', validation: '', is_user_writable: 0}
	//		},
	//		'partners': {
	//			_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
	// 			name: {type: 'string', validation: 'max:65535', is_user_writable: 1},
	// 			created_at: {type: 'date', validation: '', is_user_writable: 0},
	// 			created_by: {type: 'date', validation: '', is_user_writable: 0}
	//		},
	//	}
	// .
	// data_schema: { },
	// privilege_transit provides for a user with access to one entity to access a second entity owned or controlled by the first.
	// privilege_transit is an object whose subobjects represent the classes/services of the application.
	// Each of those subobjects is an array.
	// Each of those arrays contains a series of relationship objects.
	// Each relationship object specifies a key (the field name in the current class pointing to the parent entity) and a class (the class/service of the parent entity). A parent entity is the record in the specified target class whose value for the security service id_name field matches that of the specified key field.
	// Example:
	// 	privilege_transit: {
	// 		'ships': [{key: 'owner_id', 'class': 'partners'}]
	//	}
	// .
	// So somebody with an access level of 6 on partner P1 also has an access level of 6 on ship S1 if the owner_id on ship S1 is P1.
	// privilege_transit: { },
	tokenizeValidationRule: function(rule) {
		// This breaks a validation rule in flat text format into an array of arrays for easier access.
		// Each element of the return value contains an array containing the name of the subrule and, if present, the parameter.
		var getToken = function(text, offset) {
			// This returns the index of the next character after the token or a negative number if there is an error.
			var toffset = offset;
			while (toffset < text.length && text[toffset] != '|' && text[toffset] != ':') {
				if (text[offset] == '\'') {
					toffset++;
					while (toffset < text.length && text[offset] != '\'') {
						if (text[offset] == '\\' && toffset + 1 < text.length) {
							toffset++;
						}
						toffset++;
					}
					if (toffset < text.length && text[offset] == '\'') {
						// We found the closing quote.
						toffset++;
					} else if (toffset == text.length) {
						// console.log("Missing closing quote.");
						return -1;
					}
				} else {
					toffset++;
				}
			}
			// console.log("Token: ", offset, toffset, ".");
			return toffset;
		};
		var getRule = function(text, offset) {
			var rv = [];
			var name_start = offset;
			var name_end = getToken(text, name_start);
			if (name_end > name_start) {
				rv.push(text.slice(name_start, name_end));
				if (name_end < text.length && text[name_end] == ':') {
					var value_start = name_end + 1;
					var value_end = getToken(text, value_start);
					if (value_end > value_start) {
						rv.push(text.slice(value_start, value_end));
					}
					return [value_end, rv];
				}
			}
			return [name_end, rv];
		};
		var tokenize = function(text) {
			var offset = 0;
			var rules = [];
			var tmpv;
			while (offset < text.length) {
				tmpv = getRule(text, offset);
				offset = tmpv[0];
				if (tmpv[1].length > 0) rules.push(tmpv[1]);
				if (offset < text.length) {
					if (text[offset] == '|') {
						offset++;
					} else {
						offset = text.length;
					}
				}
			}
			return rules;
		};
		return tokenize(rule);
	},
	validateText: function(rule, data) {
		// This validates the text in data according to the rule.
		// The rule is a string.
		// The rule contains subrules.
		// Each subrule has a name.
		// It may also have a single parameter, separated from the name by a colon.
		// The pipe operator separates subrules.
		// In spite of that, the logic is of the AND variety.
		// The check passes if the data meet all subrules.
		// The max option checks that the length of the data is under that specified by the parameter.
		// The alpha_num option checks that all characters in data match /^[A-Za-z0-9]*$/.
		// The alpha_dash option checks that all characters in data match /^[A-Za-z0-9_-]*$/.
		// The alpha_slash option checks that all characters in data match /^[A-Za-z0-9\/\._-]*$/.
		// The us_date option checks that all characters in data match /^[A-Za-z0-9_-]*$/.
		// The required option checks that data is supplied. A blank string passes this check.
		// Subrules are rules, perhaps, but it seemed important to make a distinction between the input rule string and the specific rules (thus subrules).
		var rules = this.tokenizeValidationRule(rule);
		// console.log("Rules:", rules, ".");
		var rv = 0;
		if (data == null) {
			var rule_required = 0;
			rules.forEach(function (currentValue, index, array) {
				if (currentValue.length >= 1 && currentValue[0] == "required") {
					rule_required = 1;
				}
			});
			if (rule_required) return -1;
			return 0;
		}
		rules.forEach(function (currentValue, index, array) {
			if (currentValue.length >= 2 && currentValue[0] == "max") {
				// console.log("max", currentValue[1]);
				if (data.length > currentValue[1]) {
					rv = -1;
					// console.log("Fail max rule.");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_num") {
				var alpha_num_pattern = /^[A-Za-z0-9]*$/;
				var alpha_num_matches = data.match(alpha_num_pattern);
				if (alpha_num_matches == null || alpha_num_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_num rule.");
					// console.log("Matches: ", alpha_num_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_dash") {
				var alpha_dash_pattern = /^[A-Za-z0-9_-]*$/;
				var alpha_dash_matches = data.match(alpha_dash_pattern);
				if (alpha_dash_matches == null || alpha_dash_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_dash rule.");
					// console.log("Matches: ", alpha_dash_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_slash") {
				var alpha_slash_pattern = /^[A-Za-z0-9\/\._-]*$/;
				var alpha_slash_matches = data.match(alpha_slash_pattern);
				if (alpha_slash_matches == null || alpha_slash_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_slash rule.");
					// console.log("Matches: ", alpha_slash_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "us_date") {
				var us_date_pattern = /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]+$/;
				var us_date_matches = data.match(us_date_pattern);
				if (us_date_matches == null || us_date_matches.length == 0) {
					rv = -1;
					// console.log("Fail us_date rule.");
					// console.log("Matches: ", us_date_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "required") {
				if (data == null || data == undefined) rv = -1;
			}
		});
		return rv;
	},
	checkTypes: function(schema, data, exclusive, connections, target_user, dramatic) {
		// This is complementary to the validator.
		// This function checks all fields in data and rejects if it contains any typed differently from the schema. If the exclusive flag is set, this function also rejects the input if it contains any field absent from the schema.
		var extraChecks = [];
		// TODO: switch to let.
		var security = this;
		for (var pkey in data) {
			if (pkey in schema) {
				if ('type' in schema[pkey] && typeof(data[pkey]) != schema[pkey].type &&
						!('allownull' in schema[pkey] && schema[pkey]['allownull'] && data[pkey] == null)) {
					// Type mismatch.
					if (dramatic) {
						// console.log("Attempting to write a mismatched type for", pkey, ".");
						// console.log("Have", typeof(data[pkey]), ", want", schema[pkey].type, ".");
						return Promise.reject(new Error("Attempting to write a mismatched type."));
					} else {
						// console.log("Attempting to write a mismatched type for", pkey, ".");
						// console.log("Have", typeof(data[pkey]), ", want", schema[pkey].type, ".");
						return Promise.resolve(-1);
					}
				} else if ('is_user_writable' in schema[pkey] && schema[pkey].is_user_writable == 0) {
					// Unwritable field.
					if (dramatic) {
						return Promise.reject(new Error("Attempting to write to an unwritable field."));
					} else {
						// console.log("Attempting to write to an unwritable field.");
						return Promise.resolve(-1);
					}
				} else if ('target_class' in schema[pkey]) {
					// If we are checking authority or references, we chain another promise after the reference check.
					var tmparray = [pkey];
					tmparray.forEach(function (currentValue, index, array) {
						var pkk = currentValue;
						// console.log(pkk, "has target_class", schema[pkk]['target_class'], ".");
						// If the schema specify target_authority, we check (later) that the target_user (if supplied to this function) can access the pointed record.
						var check_authority = ('target_authority' in schema[pkk] && target_user != null && target_user != undefined);
						// If the schema specify checking for recursive references via a field, we check that (later).
						var check_recursion = ('recursive_reference_check' in schema[pkk] && schema[pkk]['recursive_reference_check'] > 0);
						var tname = security.id_name;
						var refquery = {};
						refquery[tname] = data[pkk];
						// console.log("Reference query:", refquery, "."); 
						extraChecks.push(security.app.service(schema[pkk]['target_class']).find({query: refquery}).then(function (resfind) {
							// console.log("Reference check result:", resfind['data'], resfind['data'].length, ".");
							// console.log("Returning", {kname: pkk, result: ((resfind['data'].length > 0) ? 0 : -1)}, ".");
							return (('data' in resfind && resfind['data'].length > 0) ? 0 : -1);
						}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; }}).then(function (fresult) {
							if (fresult == 0) {
								var mchecks = [];
								if (check_authority) {
									// console.log("Checking authority for user", target_user, "on", schema[pkk]['target_class'], data[pkk], "according to field", pkk, ".");
									mchecks.push(security.accessLevelSlow(target_user, schema[pkk]['target_class'], data[pkk]).then(function (privlev) {
										// console.log("Authority:", (((privlev & schema[pkk]['target_authority']) >= schema[pkk]['target_authority']) ? 0 : -1));
										return (((privlev & schema[pkk]['target_authority']) >= schema[pkk]['target_authority']) ? 0 : -1);
									}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; } }));
								}
								if (check_recursion) {
									mchecks.push(security.checkRecursiveDocumentDepth(schema[pkk]['target_class'], data[pkk], pkk, {}, null).then(function (reclev) {
										// console.log("Depth:", ((reclev >= 0 && reclev < 0xFFFF) ? 0 : -1));
										return ((reclev >= 0 && reclev < 0xFFFF) ? 0 : -1);
									}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; } }));
								}
								return Promise.all(mchecks).then(function (checkresults) {
									var rv = 0;
									// console.log("Authority and depth for", pkk, ":", checkresults, ".");
									checkresults.forEach(function (currentValue, index, array) { if (currentValue < 0) rv = -1; });
									return rv;
								});
							} else {
								// console.log("Skipping authority check and returning", fresult);
								return fresult;
							}
						}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; } }));
					});
				}
			} else {
				if (exclusive) {
					// Undocumented field.
					if (dramatic) {
						return Promise.reject(new Error("Attempting to write to an undocumented field."));
					} else {
						// console.log("Attempting to write to an undocumented field.");
						return Promise.resolve(-1);
					}
				}
			}
		}
		// Now we use the traditional validation (roughly equivalent to the feathers-validator but implemented internally).
		// This runs on all schema items (including those absent from the input object) so as to be able to check for required values.
		for (var skey in schema) {
			if ('validation' in schema[skey]) {
				if ('allownull' in schema[skey] && schema[skey]['allownull'] && skey in data && data[skey] == null) {
					// console.log("Null value allowed.");
				} else if (security.validateText(schema[skey]['validation'], ((skey in data) ? data[skey] : null)) < 0) {
					// console.log("Validation failed on", skey, ".");
					if (dramatic) return Promise.reject(new Error("Validation failed."));
					return Promise.resolve(-1);
				}
			}
		}
		// Unite all of the outstanding promises and return the AND composite.
		return Promise.all(extraChecks).then(function (checkresults) {
			// console.log(checkresults);
			var rv = 0;
			checkresults.forEach(function (currentValue, index, array) { if (currentValue < 0) rv = -1; });
			// console.log("Returning a complete promise.");
			return Promise.resolve(rv);
		});
	},
	generateValidatorSchema: function(schema) {
		// This converts the internal validator schema/rules to something compatible with feathers-validator.
		var rv = {};
		for (var tkey in schema) {
			rv[tkey] = schema[tkey].validation;
		}
		return rv;
	},
	checkAuthorityEntry: function(data, target_user, dramatic) {
		var target_class = null;
		if ('target_class' in data && this.validateText('max:255|alpha_dash', data['target_class']) >= 0 &&
				data['target_class'] in this.data_schema && typeof(this.data_schema[data['target_class']]) == 'object' &&
				'target_id' in data &&
				((!('validation' in this.data_schema[data['target_class']].fields[this.id_name])) || this.validateText(this.data_schema[data['target_class']].fields[this.id_name]['validation'], data['target_id']) >= 0) &&
				'privilege' in data && typeof(data['privilege']) == 'number') {
			var dyn_schema = {
				_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
				user_id: {type: 'string', validation: 'max:255|alpha_dash|required', is_user_writable: 1, target_class: 'users'},
				target_class: {type: 'string', validation: 'max:255|alpha_dash|required', is_user_writable: 1},
				target_id: {type: 'string', validation: 'max:255|alpha_dash|required', is_user_writable: 1, target_class: data['target_class'], target_authority: 14},
				privilege: {type: 'number', validation: 'required', is_user_writable: 1},
				created_at: {type: 'date', validation: '', is_user_writable: 0},
				created_by: {type: 'date', validation: '', is_user_writable: 0}
			};
			return this.checkTypes(dyn_schema, data, 1, 1, target_user, dramatic);
		}
		return Promise.resolve(-1);
	},
	checkAuthorityRevocation: function(target_id, target_user) {
		// This checks whether a user has the right to cancel an authority record.
		// The user can do this if he has control (direct or indirect) of the target of the authority record.
		// We first identify the target of the authority record.
		var security = this;
		return this.app.service('authorities').get(target_id).then(function (tr) {
			// console.log("We found the authority record.");
			if ('target_class' in tr && 'target_id' in tr &&
					security.validateText('max:255|alpha_dash', tr['target_class']) >= 0 &&
					(!('validation' in security.data_schema[tr['target_class']].fields[security.id_name])) || security.validateText(security.data_schema[tr['target_class']].fields[security.id_name]['validation'], tr['target_id']) >= 0) {
				// Now we check the permissions on that target.
				return (security.accessLevelSlow(target_user, tr['target_class'], tr['target_id']).then(function (x) {
					if ((x & 8) > 0) {
						return 0;
					} else {
						return -1;
					}
				}));
			} else {
				// console.log("The reference breaks the rules.");
				return Promise.resolve(-1);
			}
		}, function(err) { /* console.log("Something wrong!", err); */ return Promise.resolve(-1); });
	},
	checkRecursiveDocumentDepth: function(target_class, doc_id, reference_name, lookback, trail) {
		// With ES6, perhaps we can use default arguments.
		// reference_name = 'base_id', lookback = {}
		// This confirms the validity and non-loopiness of the document reference chain.
		if (doc_id in lookback && lookback[doc_id]) return -2; // This means that we have a loop.
		if (Object.keys(lookback).length >= 0xFFFF) return -3; // This means that the chain is dangerously long.
		var qresult1p = this.app.service(target_class).get(doc_id); // Get the record that we want to check.
		var security = this;
		return qresult1p.then(function (curr_entity) {
			lookback[doc_id] = 1;
			if (trail != null) trail.push(doc_id);
			if (reference_name in curr_entity && typeof(curr_entity[reference_name]) == 'string' && curr_entity[reference_name] != '') {
				// If the chain continues, we attach another promise to this one.
				return security.checkRecursiveDocumentDepth(target_class, curr_entity[reference_name], reference_name, lookback, trail).then(function (tmpv) {
					if (tmpv < 0) return tmpv;
					return tmpv + 1;
				}, function(err) {console.error('Query error in checkSaleDocumentDepth.', err); return -1;});
			}
			return Promise.resolve(0);
		}, function(err) {console.error('Query error in checkSaleDocumentDepth.', err); return -1;});
	},
	getDocumentStack: function(target_class, doc_id, reference_name) {
		var ds = [];
		var lb = {};
		var security = this;
		return security.checkRecursiveDocumentDepth(target_class, doc_id, reference_name, lb, ds).then(function () {
			// console.log(ds);
			return Promise.resolve(ds);
		}, function (err) { return Promise.reject(err); } );
	},
	getDocumentBase: function(target_class, doc_id, reference_name) {
		var ds = [];
		var security = this;
		return security.checkRecursiveDocumentDepth(target_class, doc_id, reference_name, ds).then(function () { return ds[ds.length - 1]; },
		function (err) { return Promise.reject(err); } );
	},
	getParents: function(target_class, target_id) {
		if (target_class in this.privilege_transit) {
			// Get the current record so that we can check for upstream links.
			var qresult2p = this.app.service(target_class).get(target_id); // TODO: Change to let.
			var security = this;
			return qresult2p.then(function (curr_entity) {
				var rv = []; // TODO: Change to let.
				// Iterate through all privilege transit entries for the current class.
				security.privilege_transit[target_class].forEach( function (currentValue, index, array) {
					// If there is a link in the current record to a record from a higher-order-privilege class, we add the corresponding class and identifier.
					if (currentValue['key'] in curr_entity && curr_entity[currentValue['key']] != null) {
						rv.push({'target_class': currentValue['class'], 'target_id': curr_entity[currentValue['key']]});
					}
				});
				// console.log(rv);
				return rv;
			}, function(err) {console.error('Query error in getParents.', err); return [];});
		}
		return Promise.resolve([]);
	},
	accessLevelSlow: function(target_user, target_class, target_id) {
		// This searches for direct and indirect authority links and returns the bitwise maximum authority that the specified user has over the specified item.
		// let searchParams = {user: target_user, target: target_id, '$sort': {destroyed_at: -1}};
		var searchParams = {'user_id': target_user, 'target_class': target_class, 'target_id': target_id, destroy_date: null};  // TODO: Change to let.
		// console.log("Query.", searchParams);
		var qresult1p = this.app.service('authorities').find({query: searchParams});  // TODO: Change to let.
		// Iterate through all relevant authority records for this uuid pair and find maximum privilege.
		var p1 = qresult1p.then(function (curr_auth) {
			var maxacc1 = 0;  // TODO: Change to let.
			// console.log("Queried.");
			// console.log(curr_auth);
			if ('data' in curr_auth) curr_auth['data'].forEach( function (currentValue, index, array) {
				// console.log("Result.", currentValue);
				if ('privilege' in currentValue && (typeof(currentValue['privilege']) == 'number')) {
					maxacc1 |= currentValue['privilege'];
				}
			});
			return maxacc1;
		}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
		// But we are not done. These privileges are also transitive, so we check higher order privileges.
		var security = this;
		var p2 = this.getParents(target_class, target_id).then(function (target_parents) {
			// Iterate through all privilege transit entries for the current class.
			var parent_promises = [];
			target_parents.forEach(function (currentValue, index, array) {
				// If there is a link in the current record to a record from a higher-order-privilege class, we note that parent so that we can compute privileges against that item.
				parent_promises.push(security.accessLevelSlow(target_user, currentValue['target_class'], currentValue['target_id']));
			});
			var parent_join = Promise.all(parent_promises);
			return parent_join.then(function (parent_levels) {
				// For each parent, we resolve the privilege level and add it (bitwise) to the accumulator.
				var parent_max_acc = 0; // TODO: Change to let.
				// We drop the lowest bit, as it is non-transitive.
				parent_levels.forEach(function (currentValue, index, array) { parent_max_acc |= (currentValue & ~1); });
				return parent_max_acc;
			}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
		}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
		// Now we combine the results of the immediate and parent privilege computations.
		var acc_join = Promise.all([p1, p2]);
		return acc_join.then(function (parent_levels) {
			var parent_max_acc = 0; // TODO: Change to let.
			parent_levels.forEach(function (currentValue, index, array) { parent_max_acc |= currentValue; });
			// console.log("Access level:", parent_max_acc);
			return parent_max_acc;
		}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
	},
	userCanRead: function(target_user, target_class, target_id) {
		return (this.accessLevelSlow(target_user, target_class, target_id).then(function (x) {
			return ((x & 0x3) ? 1 : 0);
		}, function (err) {return 0;}));
	},
	userCanWrite: function(target_user, target_class, target_id) {
		return (this.accessLevelSlow(target_user, target_class, target_id).then(function (x) {
			return (x & 6) == 6;
		}, function (err) {return 0;}));
	},
	userCanListChildren: function(target_user, target_class, target_id) {
		return (this.accessLevelSlow(target_user, target_class, target_id).then(function (x) {
			return ((x & 0x2) ? 1 : 0);
		}, function (err) {return 0;}));
	},
	userCanAddChildren: function (target_user, target_class, target_id) {
		return this.userCanWrite(target_user, target_class, target_id);
	},
	pickPathFieldsBase: {
		_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
		user_id: {type: 'string', validation: 'max:255|alpha_dash', is_user_writable: 1},
		pick_path: {type: 'object', is_user_writable: 1},
		created_at: {type: 'date', validation: '', is_user_writable: 0},
		created_by: {type: 'string', validation: 'max:255|alpha_dash', is_user_writable: 0},
		destroyed_at: {type: 'date', validation: '', is_user_writable: 0},
		destroyed_by: {type: 'date', validation: '', is_user_writable: 0}
	},
	serviceNameArrayFromPickPathTemplate: function(pickPathTemplate) {
		var output = [];
		pickPathTemplate.forEach(function(val, ind, arr) {
			// If the pick path template entry specifies a class and the entry is not flagged as virtual, add the class name.
			if ('class' in val && !('virtual' in val && val['virtual']))
				output.push(val['class']);
		});
		return output;
	},
	serviceNameObjectFromPickPathTemplate: function(pickPathTemplate) {
		var output = {};
		pickPathTemplate.forEach(function(val, ind, arr) {
			// If the pick path template entry specifies a class and the entry is not flagged as virtual, add the class name.
			if ('class' in val && !('virtual' in val && val['virtual']))
				output[val['class']] = 1;
		});
		return output;
	},
	crossServiceMultiGet: function(allowedServices, inputSeq) {
		var fetchers = [];
		var thisC = this;
		inputSeq.forEach(function(val, ind, arr) {
			if ('class' in val && 'id' in val && typeof(val['class']) == 'string' && typeof(val.id) == 'string') {
				// We need to validate the data types and formats before using them.
				var alpha_dash_pattern = /^[A-Za-z0-9_-]+$/;
				var class_matches = val['class'].match(alpha_dash_pattern);
				var id_matches = val.id.match(alpha_dash_pattern);
				if (class_matches != null && class_matches.length > 0 && id_matches != null && id_matches.length > 0 &&
						val['class'] in allowedServices && allowedServices[val['class']] == 1) {
					// Fetch the record.
					fetchers.push(thisC.app.service(val['class']).get(val.id));
				} else {
					// So as to allow virtual pick path items, we gloss over missing services and let the caller figure things out.
					// fetchers.push(Promise.reject(new Error("Bad pick path format.")));
					fetchers.push({});
				}
			}
		});
		return Promise.all(fetchers);
	},
	pickPathCheck: function(pickPathTemplate, inputPath) {
		// We check the length of the incoming path before comparing to the template.
		if (inputPath.length > pickPathTemplate.length) return Promise.reject(new Error("Pick path too long."));
		var errFlag = 0;
		// We check that the classes in the pick path match those in the template.
		inputPath.forEach(function (val, ind, arr) {
			// console.log(val, pickPathTemplate[ind]);
			if (!('class' in val && typeof(val['class']) == 'string' && val['class'] == pickPathTemplate[ind]['class']))
				errFlag |= 1;
		});
		if (errFlag) return Promise.reject(new Error("Pick path invalid."));
		// We fetch the referenced records.
		return this.crossServiceMultiGet(this.serviceNameObjectFromPickPathTemplate(pickPathTemplate), inputPath).then(function (recs) {
			// We check the chain of references.
			var idCache = null;
			recs.forEach(function (val, ind, arr) {
				if (ind > 0 && 'parent_id' in pickPathTemplate[ind] &&
						(!(pickPathTemplate[ind]['parent_id'] in val) || val[pickPathTemplate[ind]['parent_id']] != inputPath[ind - 1].id)) {
					errFlag |= 1;
				}
			});
			if (errFlag) return Promise.reject(new Error("Pick path discontinuous."));
			return Promise.resolve(0);
		}, function (err) { return Promise.reject(err); });
	},
	hookPickPathCreateAuth: function (hook) {
		// console.log("hookPickPathCreateAuth");
		// The incoming hook must have the pick path schema. Specifically, it must be at hook.custom.pickPathTemplate.
		// Bypass authorization for internal queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if (!('custom' in hook && 'pickPathTemplate' in hook.custom)) return Promise.reject(new Error("No pick path in hook."));
		// Check that necessary data are present and that the authorized user is the one creating the record.
		if ('pickPathTemplate' in hook.custom &&
				'data' in hook && 'user' in hook.params && 'user_id' in hook.data && hook.data.user_id == hook.params.user[hook.app.security.id_name]) {
			return hook.app.security.checkTypes(hook.app.security.pickPathFieldsBase, hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1).then(
				function (typeR) {
					return hook.app.security.pickPathCheck(hook.custom.pickPathTemplate, hook.data.pick_path).then(
						function (pathR) {
							if (typeR == 0 && pathR == 0)
								return Promise.resolve(hook);
							return Promise.reject(new Error("Checks failed unusually on the new pick path record."));
						},
						function (err) {
							return Promise.reject(err);
						}
					);
				},
				function (err) {
					return Promise.reject(err);
				}
			);
		}
		return Promise.reject(new Error("Invalid parameters."));
	},
	hookPickPathViewAuth: function (hook) {
		// console.log("hookPickPathViewAuth");
		// Bypass authorization for internal queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		// Check that necessary data are present and that the authorized user is the one viewing the record.
		return this.get(hook.id).then(
			function(res) {
				if (res.user_id == hook.params.user[hook.app.security.id_name]) return Promise.resolve(hook);
				return Promise.reject(new Error("No access to this record."));
			},
			function(err) {
				return Promise.reject(new Error("No access to this record."));
			}
		);
	},
	hookPickPathDestroyAuth: function (hook) {
		// console.log("hookPickPathDestroyAuth");
		// Bypass authorization for internal queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		// Check that necessary data are present and that the authorized user is the one viewing the record.
		return this.get(hook.id).then(
			function(res) {
				if (res.user_id == hook.params.user[hook.app.security.id_name]) return Promise.resolve(hook);
				return Promise.reject(new Error("No access to this record."));
			},
			function(err) {
				return Promise.reject(new Error("No access to this record."));
			}
		);
	},
	hookPickPathViewPostAuth: function (hook) {
		// console.log("hookPickPathViewPostAuth");
		// Bypass authorization for internal queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		// Check that necessary data are present and that the authorized user is the one viewing the record.
		if ('user_id' in hook.result && hook.result.user_id == hook.params.user[hook.app.security.id_name]) return Promise.resolve(hook);
		return Promise.reject(new Error("No access to this record."));
	},
	hookPickPathFindAuth: function (hook) {
		// console.log("hookPickPathFindAuth");
		// console.log("typeof(hook) =", typeof(hook), ".");
		// Bypass authorization for internal queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if ('query' in hook.params && 'user_id' in hook.params.query && hook.params.query.user_id == hook.params.user[hook.app.security.id_name]) return Promise.resolve(hook);
		return Promise.reject(new Error("A user can search only his own pick paths."));
	},
	hookPickPathDestroyOld: function (hook) {
		// We want to find any active pick paths for the current user and to destroy them.
		// There is no authority checking. This is for calling only by the other hooks.
		var thisC = this;
		return thisC.find({query: {user_id: hook.data.user_id, destroyed_at: null}}).then(
			function (sresults) {
				var tresults = [];
				sresults.data.forEach(function (val, ind, arr) {
					// We sneakily construct a fake hook so that we can call hookSoftDestroy.
					// It uses this, so we must set it via call or apply.
					var thook = {};
					thook.id = val[hook.app.security.id_name];
					thook.params = {user: hook.params.user};
					tresults.push(hook.app.security.hookSoftDestroy.apply(thisC, [thook]));
				});
				return Promise.all(tresults).then(
					function (uresults) {
						return hook;
					},
					function (err) {
						return Promise.reject(err);
					}
				);
			},
			function (err) {
				return Promise.reject(err);
			}
		);
	},
	hookSoftDestroy: function (hook) {
		return this.patch(hook.id, { destroyed_at: new Date() , destroyed_by: hook.params.user[hook.app.security.id_name] }, hook.params).then(function(data) {
			// Set the result from `patch` as the method call result
			hook.result = data;
			// Always return the hook or `undefined`
			return hook;
		});
	},
	hookPickPathCreate: function (hook) {
		// Bypass all this if the request is internal.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return hook;
		var thisC = this;
		return hook.app.security.hookPickPathCreateAuth(hook).then(
			function (hook1) {
				// We destroy the old entries, if any, and then add the creation information and the destruction fields to this record.
				// Since hookAddCreationDestructionInPlaceBefore is realtime, it would be simpler to do this in reverse order.
				// But then the destroy date for the old record would be after the creation date of the new record.
				return hook.app.security.hookPickPathDestroyOld.apply(thisC, [hook]).then( function(hook2) {
					return hook.app.security.hookAddCreationDestructionInPlaceBefore.apply(thisC, [hook]);
				}, function (err) { return Promise.reject(err); });
			},
			function (err) {
				return Promise.reject(err);
			}
		);
	},
	hookPickPathDestroy: function (hook) {
		// Bypass all this if the request is internal.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return hook;
		var thisC = this;
		return hook.app.security.hookPickPathDestroyAuth(hook).then(
			function (hook1) {
				return hook.app.security.hookSoftDestroy.apply(thisC, [hook]);
			},
			function (err) {
				return Promise.reject(err);
			}
		);
	},
	hookAddCreationInformationInPlaceBefore: function(hook) {
		// console.log("Adding creation information.");
		hook.data.created_at = new Date();
		if ('user' in hook.params) hook.data.created_by = hook.params.user[hook.app.security.id_name];
		// console.log("Added creation information.");
		return hook;
	},
	hookAddDestructionSpaceInPlaceBefore: function(hook) {
		// console.log("Adding creation information.");
		hook.data.destroyed_at = null;
		if ('user' in hook.params) hook.data.destroyed_by = null;
		// console.log("Added creation information.");
		return hook;
	},
	hookAddDestructionInformationInPlaceBefore: function(hook) {
		// We use this only when creating an already destroyed record.
		// console.log("Adding creation information.");
		hook.data.destroyed_at = new Date();
		if ('user' in hook.params) hook.data.destroyed_by = hook.params.user[hook.app.security.id_name];
		// console.log("Added creation information.");
		return hook;
	},
	hookAddCreationDestructionInPlaceBefore: function(hook) {
		hook.app.security.hookAddCreationInformationInPlaceBefore.apply(this, [hook]);
		hook.app.security.hookAddDestructionSpaceInPlaceBefore.apply(this, [hook]);
		return hook;
	},
	hookUserCanRead: function(target_class, hook) {
		// This is for use with get queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if (hook.id == null) return Promise.reject(new Error("Must have record id."));
		return (this.userCanRead(hook.params.user[hook.app.security.id_name], target_class, hook.id).then(function (iv) {
			if (iv > 0) {
				return Promise.resolve(hook);
			} else {
				return Promise.reject(new Error("No access."));
			}
		}));
	},
	hookUserCanWrite: function(target_class, hook) {
		// This is for use with patch, update, and remove queries. So it is seldom used.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if (hook.id == null) return Promise.reject(new Error("Must have record id."));
		return (this.userCanWrite(hook.params.user[hook.app.security.id_name], target_class, hook.id).then(function (iv) {
			if (iv > 0) {
				return Promise.resolve(hook);
			} else {
				return Promise.reject(new Error("No access."));
			}
		}));
	},
	hookUserCanListChildren: function(target_class, parent_class, parent_id_name, hook) {
		// This is for use with find queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if ('query' in hook.params && parent_id_name in hook.params.query && (typeof(hook.params.query[parent_id_name]) == 'number' || typeof(hook.params.query[parent_id_name]) == 'string')) {
			return hook.app.security.userCanListChildren(hook.params.user[hook.app.security.id_name], parent_class, hook.params.query[parent_id_name]).then(function(iv) {
				if (iv > 0) {
					return Promise.resolve(hook);
				} else {
					return Promise.reject(new Error("No access."));
				}
			});
		} else {
			// console.log("Users may only find " + target_class + " for specific " + parent_class + ".");
			return Promise.reject(new Error("The query must specify the parent."));
		}
	},
	hookUserCanAddChildren: function(target_class, parent_class, parent_id_name, hook) {
		// This is for use with find queries.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if ('query' in hook.params && parent_id_name in hook.params.query && (typeof(hook.params.query[parent_id_name]) == 'number' || typeof(hook.params.query[parent_id_name]) == 'string')) {
			return hook.app.security.userCanAddChildren(hook.params.user[hook.app.security.id_name], parent_class, hook.params.query[parent_id_name]).then(function(iv) {
				if (iv > 0) {
					return Promise.resolve(hook);
				} else {
					return Promise.reject(new Error("No access."));
				}
			});
		} else {
			// console.log("Users may only change " + target_class + " for specific " + parent_class + ".");
			return Promise.reject(new Error("The query must specify the parent."));
		}
	},
	hookUserCanAddChildrenWrite: function(target_class, parent_class, parent_id_name, hook) {
		// This is for use with creations, patches, and destructions.
		// But checkTypes is more comprehensive.
		// It is like hookUserCanAddChildren, but with hook.params.query replaced with hook.data.
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		// console.log(hook.data);
		// console.log(parent_id_name);
		if ('data' in hook && parent_id_name in hook.data && (typeof(hook.data[parent_id_name]) == 'number' || typeof(hook.data[parent_id_name]) == 'string')) {
			return hook.app.security.userCanListChildren(hook.params.user[hook.app.security.id_name], parent_class, hook.data[parent_id_name]).then(function(iv) {
				if (iv > 0) {
					return Promise.resolve(hook);
				} else {
					return Promise.reject(new Error("No access."));
				}
			});
		} else {
			// console.log("Users may only change " + target_class + " for specific " + parent_class + ".");
			return Promise.reject(new Error("The query must specify the parent."));
		}
	},
	hookCreationPreflight: function(target_class, hook) {
		if (('provider' in hook.params && hook.params['provider'] != "") || 'user' in hook.params) {
			// console.log("Checking types and access.");
			return hook.app.security.checkTypes(hook.app.security.data_schema[target_class].fields, hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1).then( function (iv) {
				// console.log("Checking access succeeded.");
				if (iv < 0) return Promise.reject(new Error("We cannot write this."));
				hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
				return Promise.resolve(hook);
			}, function (err) { return Promise.reject(err); } );
		} else {
			hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
			return Promise.resolve(hook);
		}
	},
	hookAuthorizationCanSearch: function(hook) {
    if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
    // We grant access either to a user to whom the authority belongs or to a user who has read access to the underlying object.
    var access_promises = [];
    if ('query' in hook.params && 'user' in hook.params) {
      if ('user_id' in hook.params.query) {
        access_promises.push(Promise.resolve(hook.params.user[hook.app.security.id_name] == hook.params.query['user_id']));
      }
      if ('target_class' in hook.params.query && 'target_id' in hook.params.query) {
        access_promises.push(userCanRead(hook.params.user[hook.app.security.id_name], hook.params.query['target_class'], hook.params.query['target_id']));
      }
    }
    return Promise.all(access_promises)
    .then(function (access_results) {
      access_results.forEach( function (val, ind, arr) {
        if (val) return Promise.resolve(hook);
      });
      return Promise.reject(new Error("Find queries must be limited to the querying user as user_id or to a target_class + target_id combination to which the querying user has access."));
    });
	},
	hookAuthorizationCanRead: function(hook) {
    if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		// var service = this; // This probably works.
		return hook.app.service('authorities').get(hook.id).then( function (auth_rec) {
		  // We grant access either to a user to whom the authority belongs or to a user who has read access to the underlying object.
		  var access_promises = [];
		  if ('user' in hook.params) {
		    if ('user_id' in auth_rec) {
		      access_promises.push(Promise.resolve(hook.params.user[hook.app.security.id_name] == auth_rec['user_id']));
		    }
		    if ('target_class' in auth_rec && 'target_id' in auth_rec) {
		      access_promises.push(userCanRead(hook.params.user[hook.app.security.id_name], auth_rec['target_class'], auth_rec['target_id']));
		    }
		  }
		  return Promise.all(access_promises)
		  .then(function (access_results) {
		    access_results.forEach( function (val, ind, arr) {
		      if (val) return Promise.resolve(hook);
		    });
		    return Promise.reject(new Error("Access is limited to the beneficiary of the authority record and to those who can see the target of the authority record."));
		  });
		});
	},
	hookAuthorizationPatch: function(hook) {
    if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		return Promise.reject(new Error("Patching authority records is not a thing."));
	},
	hookAuthorizationPreflight: function(hook) {
		if (('provider' in hook.params && hook.params['provider'] != "") || 'user' in hook.params) {
			return hook.app.security.checkAuthorityEntry(hook.data, hook.params.user[hook.app.security.id_name], 1).then( function (iv) {
				if (iv < 0) return Promise.reject(new Error("We cannot write this."));
				hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
				hook.data.destroyed_at = null;
				return Promise.resolve(hook);
			});
		} else {
			hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
			if (!('destroyed_at' in hook.data)) { hook.data.destroyed_at = null; }
			return Promise.resolve(hook);
		}
	},
	hookAuthorizationRevocationOverride: function(hook, service) {
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if (hook.id == null) return Promise.reject(new Error("Must have record id."));
		return hook.app.security.checkAuthorityRevocation(hook.id, hook.params.user[hook.app.security.id_name]).then( function (iv) {
			if (iv >= 0) {
				// We copy some stuff from the Feathers documentation.
				return service.patch(hook.id, { destroyed_at: new Date() }, hook.params).then(function(data) {
					// Set the result from `patch` as the method call result
					hook.result = data;
					// Always return the hook or `undefined`
					return hook;
				});
			} else {
				hook.result = {};
				return Promise.resolve(hook);
			}
		});
	}
};

function hookDoWithReadLock(hook, tododo) {
	// tododo must always return a promise, even on an error, or the lock gets stuck.
	// if ('lock' in hook) throw new Error("This hook event already has a lock!");
	if ('lock' in hook && 'success' in hook.lock && hook.lock.success == 1) return Promise.resolve(tododo());
	if (!('provider' in hook.params && hook.params['provider'] != "")) {
		// If the access is internal, we assume that necessary locks are already acquired.
		return Promise.resolve(tododo());
	} else {
		hook.lock = {};
		// We create a promise, to resolve when the lock is in place.
		hook.lock.promise = new Promise( function(resolve, reject) {
			hook.lock.resolve = resolve;
			hook.lock.reject = reject;
		});
		// We request a lock.
		hook.app.lock.readLock(function (release) {
			// Once we have the lock, we store the release function.
			hook.lock.release = release;
			hook.lock.success = 1;
			// And we resolve the locking promise so that the query can continue.
			// But we also release the lock if the inner work fails.
			hook.lock.resolve(tododo().then(
				function (rv) { return Promise.resolve(rv); },
				function (err) {
					release();
					hook.lock.success = 0;
					delete hook.lock.release;
					return Promise.reject(err);
				}
			));
		});
		// We return that promise.
		// That promise resolves to another promise (from the reprivileger) that ultimately resolves back to the hook.
		return hook.lock.promise;
	}
	return Promise.reject(new Error("Something is wrong here."));
}

function doPromiseWithReadLock(lock, tododo) {
	var local_lock = {};
	// We create a promise, to resolve when the lock is in place.
	local_lock.promise = new Promise( function(resolve, reject) {
		local_lock.resolve = resolve;
		local_lock.reject = reject;
	});
	// We request a lock.
	lock.readLock(function (release) {
		// This runs when the lock becomes available.
		// We resolve the locking promise to the query result, but we inject a release instruction.
		// console.log("Have lock.");
		var tmpp = tododo();
		// console.log("Work product.");
		// console.log(tmpp);
		return tmpp.then(function(rv) { /* console.log("Protected work done."); */ local_lock.resolve(rv); /* console.log("Releasing."); */ release(); console.log("Returning."); return Promise.resolve(rv); }, function(err) { local_lock.reject(err); release(); return Promise.reject(err); });
	});
	// We return that promise.
	// console.log("The promise of a lock is henceforth made.");
	return local_lock.promise;
}

function hookDoWithWriteLock(hook, tododo) {
	// tododo must always return a promise, even on an error, or the lock gets stuck.
	// if ('lock' in hook) throw new Error("This hook event already has a lock!");
	if ('lock' in hook && 'success' in hook.lock && hook.lock.success == 1) return Promise.resolve(tododo());
	if (!('provider' in hook.params && hook.params['provider'] != "")) {
		// If the access is internal, we assume that necessary locks are already acquired.
		return Promise.resolve(tododo());
	} else {
		hook.lock = {};
		// We create a promise, to resolve when the lock is in place.
		hook.lock.promise = new Promise( function(resolve, reject) {
			hook.lock.resolve = resolve;
			hook.lock.reject = reject;
		});
		// We request a lock.
		hook.app.lock.writeLock(function (release) {
			// Once we have the lock, we store the release function.
			hook.lock.release = release;
			hook.lock.success = 1;
			// And we resolve the locking promise so that the query can continue.
			hook.lock.resolve(tododo().then(
				function (rv) { return Promise.resolve(rv); },
				function (err) {
					release();
					delete hook.lock.release;
					hook.lock.success = 0;
					return Promise.reject(err);
				}
			));
		});
		// We return that promise.
		// That promise resolves to another promise (from the reprivileger) that ultimately resolves back to the hook.
		return hook.lock.promise;
	}
	return Promise.reject(new Error("Something is wrong here."));
}

function doPromiseWithWriteLock(lock, tododo) {
	var local_lock = {};
	// We create a promise, to resolve when the lock is in place.
	local_lock.promise = new Promise( function(resolve, reject) {
		local_lock.resolve = resolve;
		local_lock.reject = reject;
	});
	// We request a lock.
	lock.writeLock(function (release) {
		// This runs when the lock becomes available.
		// We resolve the locking promise to the query result, but we inject a release instruction.
		return tododo().then(function(rv) { local_lock.resolve(rv); release(); return Promise.resolve(rv); }, function(err) { local_lock.reject(err); release(); return Promise.reject(err); });
	});
	// We return that promise.
	return local_lock.promise;
}

function hookUnlock(hook) {
	if (!('provider' in hook.params && hook.params['provider'] != "")) {
		// We are locking only for the top-level (outside) access, so we are unlocking only at the end of that.
		return hook;
	}
	if ('lock' in hook) {
		if ('release' in hook.lock && typeof(hook.lock.release) == 'function') {
			hook.lock.release();
		}
		delete hook.lock;
	}
	return hook;
}

function reprivilegerCreate(app) {
	var rv = {};
	for (var ell in security_template) {
		rv[ell] = security_template[ell];
	}
	rv['app'] = app;
	rv['id_name'] = '_id';
	rv['privilege_transit'] = {};
	rv['data_schema'] = {};
	return rv;
}

exports = module.exports = {create: reprivilegerCreate, hookDoWithReadLock: hookDoWithReadLock, hookDoWithWriteLock: hookDoWithWriteLock, doWithReadLock: doPromiseWithReadLock, doWithWriteLock: doPromiseWithWriteLock, hookUnlock: hookUnlock};


