// Copyright 2012-2018 by Frank Trampe.
// All rights reserved except as granted by an accompanying license.

'use strict';

var path = require('path');
var compress = require('compression');
var cors = require('cors');
var bodyParser = require('body-parser');
// var Promise = require('promise');
var express = require('express');
var ReadWriteLock = require('rwlock');
var frankenlib = require('frankenlib');

function hookTimestamp(hook) {
	var timestamp = new Date();
	if ("timestamp" in hook.params && hook.params.timestamp !== null) {
		if (timestamp instanceof Date) timestamp = hook.params.timestamp;
		else timestamp = new Date(hook.params.timestamp);
	}
	return timestamp;
}

function mergeSubmodelData(schema, data) {
	// This takes an object containing data according to the provided model and flattens the contents of subobjects into the top-level dictionary.
	// The operation presupposes non-conflicting names.
	var output = {};
	flattenHierarchy(output, null, '_', data);
	return output;
}

function dateConvertToUTC(iv) {
	// This converts a date with nominal values in the active time zone to one with nominal values in UTC.
	return new Date(iv.getUTCFullYear(), iv.getUTCMonth(), iv.getUTCDate(), iv.getUTCHours(), iv.getUTCMinutes(), iv.getUTCSeconds(), iv.getUTCMilliseconds());
}

function tokenizeValidationRule(rule) {
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
}

function validateText(rule, data) {
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
	// The alpha_slash option checks that all characters in data match /^[A-Za-z0-9\/\.:_-]*$/.
	// The us_date option checks that all characters in data match /^[A-Za-z0-9_-]*$/.
	// The required option checks that data is supplied. A blank string passes this check.
	// Subrules are rules, perhaps, but it seemed important to make a distinction between the input rule string and the specific rules (thus subrules).
	var rules = tokenizeValidationRule(rule);
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
	if (typeof('data') == 'string') {
		rules.forEach(function (currentValue, index, array) {
			if (currentValue.length >= 2 && currentValue[0] == "max") {
				// console.log("max", currentValue[1]);
				if (data.length > currentValue[1]) {
					rv = -1;
					// console.log("Fail max rule.");
				}
			} else if (currentValue.length >= 2 && currentValue[0] == "min") {
				// console.log("max", currentValue[1]);
				if (data.length < currentValue[1]) {
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
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_dash_space") {
				var alpha_dash_pattern = /^[A-Za-z0-9 _-]*$/;
				var alpha_dash_matches = data.match(alpha_dash_pattern);
				if (alpha_dash_matches == null || alpha_dash_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_dash rule.");
					// console.log("Matches: ", alpha_dash_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_slash") {
				var alpha_slash_pattern = /^[A-Za-z0-9\/\.:_-]*$/;
				var alpha_slash_matches = data.match(alpha_slash_pattern);
				if (alpha_slash_matches == null || alpha_slash_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_slash rule.");
					// console.log("Matches: ", alpha_slash_matches, ".");
				}
			} else if (currentValue.length >= 1 && currentValue[0] == "alpha_slash_space") {
				var alpha_slash_space_pattern = /^[A-Za-z0-9\/\.: _-]*$/;
				var alpha_slash_space_matches = data.match(alpha_slash_space_pattern);
				if (alpha_slash_space_matches == null || alpha_slash_space_matches.length == 0) {
					rv = -1;
					// console.log("Fail alpha_slash space_rule.");
					// console.log("Matches: ", alpha_slash_space_matches, ".");
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
	} else if (typeof('data') == 'number') {
		rules.forEach(function (currentValue, index, array) {
			if (currentValue.length >= 2 && currentValue[0] == "max") {
				// console.log("max", currentValue[1]);
				if (data > currentValue[1]) {
					rv = -1;
					// console.log("Fail max rule.");
				}
			} else if (currentValue.length >= 2 && currentValue[0] == "min") {
				// console.log("max", currentValue[1]);
				if (data < currentValue[1]) {
					rv = -1;
					// console.log("Fail max rule.");
				}
			} else if (currentValue.length >= 2 && currentValue[0] == "step") {
				if (data % currentValue[1] != 0){
					rv = -1;
					// console.log("Fail step rule.");
				}
			}
		});
	} else {
		rv = -1;
	}
	return rv;
}

function checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, operation, original, overlay_only, overrides) {
	// This is complementary to the validator.
	// This function checks all fields in data and rejects if it contains any typed differently from the schema. If the exclusive flag is set, this function also rejects the input if it contains any field absent from the schema.
	// dramatic indicates whether to fail loudly (instead of just returning -1).
	// operation indicates the type of data operation for which the data is intended.
	// original includes the original record.
	// overlay_only indicates that only the overlay data are to be updated.
	var extraChecks = [];
	// TODO: switch to let.
	var security = null;
	if (this && 'models' in this && 'app' in this)
		security = this;
	if (!('fields' in schema)) return Promise.reject("Called without fields.");
	var sfields = schema.fields;
	// console.log("Type check.");
	// console.log(data);
	for (var pkey in data) {
		if (pkey in sfields) {
			if (
					(
						('type' in sfields[pkey] &&
							(typeof(data[pkey]) != sfields[pkey].type ||
								('instanceof' in sfields[pkey] && !(data[pkey] instanceof sfields[pkey]['instanceof']))
							)
						) ||
						(
							(
								('submodel' in sfields[pkey] && sfields[pkey]['submodel'] != null) ||
								('submodel_inline' in sfields[pkey] && sfields[pkey]['submodel_inline'] != null)
							) &&
							(data[pkey] == null || !(data[pkey] instanceof Object))
						)
					) &&
					!('allownull' in sfields[pkey] && sfields[pkey]['allownull'] && data[pkey] == null) &&
					!('allow_null' in sfields[pkey] && sfields[pkey]['allow_null'] && data[pkey] == null)
				) {
				// Type mismatch.
				if (dramatic) {
					// console.log("Attempting to write a mismatched type for", pkey, ".");
					// console.log("Have", typeof(data[pkey]), ", want", sfields[pkey].type, ".");
					return Promise.reject(new Error("Attempting to write a mismatched type for " + pkey + "."));
				} else {
					// console.log("Attempting to write a mismatched type for", pkey, ".");
					// console.log("Have", typeof(data[pkey]), ", want", sfields[pkey].type, ".");
					return Promise.resolve(-1);
				}
			} else if (('is_user_writable' in sfields[pkey] && sfields[pkey].is_user_writable == 0) ||
					('is_user_writable' in overrides && overrides.is_user_writable == 0)) {
				// Unwritable field.
				if (dramatic) {
					// console.log("Attempting to write field", pkey, ", which is not writable.");
					return Promise.reject(new Error("Attempting to write to unwritable field " + pkey + "."));
				} else {
					// console.log("Attempting to write to an unwritable field.");
					return Promise.resolve(-1);
				}
			} else if (security &&
					((('administrator_only' in sfields[pkey] && sfields[pkey].administrator_only == 1) ||
					('administrator_only' in overrides && overrides.administrator_only == 1)) &&
					target_user != null &&
					(operation == 'patch' ||
					(operation == 'update' && (original == null || !(pkey in original) || data[pkey] != original[pkey])) ||
					(operation == 'create' && (!('default' in sfields[pkey]) || data[pkey] != sfields[pkey].default_value))))
					) {
				// Unwritable field for non-administrators.
				extraChecks.push(
					security.userIsAdministrator(target_user).then(
						function (is_administrator) {
							if (!(is_administrator)) {
								if (dramatic) {
									return Promise.reject(new Error("Attempting to write to administrator-only field" + pkey + "."));
								} else {
									// console.log("Attempting to write to an unwritable field.");
									return Promise.resolve(-1);
								}
							}
						}, function (err) { return Promise.reject(err); }
					)
				);
			} else if ((('immutable' in sfields[pkey] && sfields[pkey].immutable == 1) ||
					('immutable' in overrides && overrides.immutable == 1)) &&
					// target_user != null &&
					((operation == 'patch' && original && pkey in original && original[pkey] != null) ||
					(operation == 'update' && original && pkey in original && original[pkey] != null))) {
				// Immutable field.
				return Promise.reject(new Error("Attempting to change an immutable field " + pkey + "."));
			} else if (('computed' in sfields[pkey] && sfields[pkey].computed == 1) ||
					('computed' in overrides && overrides.computed == 0)) {
				// Unwritable field.
				if (dramatic) {
					return Promise.reject(new Error("Attempting to write to computed (virtual) field " + pkey + "."));
				} else {
					// console.log("Attempting to write to a computed (virtual) field.");
					return Promise.resolve(-1);
				}
			} else if (('label' in sfields[pkey] && sfields[pkey].label == 1) ||
					('label' in overrides && overrides.label == 0)) {
				// Unwritable field.
				if (dramatic) {
					return Promise.reject(new Error("Attempting to write to label field " + pkey + "."));
				} else {
					// console.log("Attempting to write to a computed (virtual) field.");
					return Promise.resolve(-1);
				}
			} else if ('submodel' in sfields[pkey] || 'submodel_inline' in sfields[pkey]) {
				var subschema = {};
				if ('submodel_inline' in sfields[pkey]) {
					subschema = sfields[pkey].submodel_inline;
				} else if ('submodel' in sfields[pkey]) {
					if (security && sfields[pkey].submodel in security.models) {
						subschema = security.models[sfields[pkey].submodel];
					} else {
						if (dramatic) {
							return Promise.reject(new Error("Bad model reference."));
						} else {
							console.log("Bad model reference.");
							return Promise.resolve(-1);
						}
					}
				}
				// Note that submodel and target_class are incompatible options for obvious reasons.
				if ('fields' in subschema) {
					var nest_overrides = {};
					var ttt;
					// Copy the overrides.
					for (ttt in overrides) { nest_overrides[ttt] = overrides[ttt]; }
					// If there is no override for is_user_writable, use the value specified in the sfields.
					if (!('is_user_writable' in overrides) && ('is_user_writable' in sfields[pkey])) {
						nest_overrides['is_user_writable'] = sfields[pkey]['is_user_writable'];
					}
					if (!('administrator_only' in overrides) && ('administrator_only' in sfields[pkey])) {
						nest_overrides['administrator_only'] = sfields[pkey]['administrator_only'];
					}
					if (!('computed' in overrides) && ('computed' in sfields[pkey])) {
						nest_overrides['computed'] = sfields[pkey]['computed'];
					}
					if (!('label' in overrides) && ('label' in sfields[pkey])) {
						nest_overrides['label'] = sfields[pkey]['label'];
					}
					if (!('overlay' in overrides) && ('overlay' in sfields[pkey])) {
						nest_overrides['overlay'] = sfields[pkey]['overlay'];
					}
					if (data[pkey] instanceof Object && data[pkey] != null) {
						if (security)
							extraChecks.push(security.checkTypesNested(subschema, data[pkey], exclusive, connections, target_user, dramatic, operation, ((original != null && pkey in original) ? original[pkey] : null), overlay_only, overrides));
						else
							extraChecks.push(checkTypesNested(subschema, data[pkey], exclusive, connections, target_user, dramatic, operation, ((original != null && pkey in original) ? original[pkey] : null), overlay_only, overrides));
					}
				} else {
					if (dramatic) {
						return Promise.reject(new Error("The model lacks a field specification."));
					} else {
						console.log("The model lacks a field specification.");
						return Promise.resolve(-1);
					}
				}
			} else if (security && 'target_class' in sfields[pkey]) {
				// If we are checking authority or references, we chain another promise after the reference check.
				var tmparray = [pkey];
				tmparray.forEach(function (currentValue, index, array) {
					var pkk = currentValue;
					// console.log(pkk, "has target_class", sfields[pkk]['target_class'], ".");
					// If the sfields specify target_authority, we check (later) that the target_user (if supplied to this function) can access the pointed record.
					var check_authority = ('target_authority' in sfields[pkk] && target_user != null && target_user != undefined);
					// If the sfields specify checking for recursive references via a field, we check that (later).
					var check_recursion = ('recursive_reference_check' in sfields[pkk] && sfields[pkk]['recursive_reference_check'] > 0);
					var tname = security.id_name;
					var refquery = {};
					refquery[tname] = data[pkk];
					// console.log("Reference query:", refquery, "."); 
					extraChecks.push(security.app.service(sfields[pkk]['target_class']).find({query: refquery}).then(function (resfind) {
						// console.log("Reference check result:", resfind['data'], resfind['data'].length, ".");
						// console.log("Returning", {kname: pkk, result: ((resfind['data'].length > 0) ? 0 : -1)}, ".");
						return (('data' in resfind && resfind['data'].length > 0) ? 0 :
								(dramatic ? Promise.reject(new Error("Missing reference target.")) : -1));
					}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; }}).then(function (fresult) {
						if (fresult == 0) {
							var mchecks = [];
							if (check_authority) {
								// console.log("Checking authority for user", target_user, "on", sfields[pkk]['target_class'], data[pkk], "according to field", pkk, ".");
								mchecks.push(security.accessLevelSlowWithUser(target_user, sfields[pkk]['target_class'], data[pkk]).then(function (privlev) {
									// console.log("Authority:", (((privlev & sfields[pkk]['target_authority']) >= sfields[pkk]['target_authority']) ? 0 : -1));
									if (!((privlev & sfields[pkk]['target_authority']) >= sfields[pkk]['target_authority'])) {
										// If the authority is insufficient, check whether the user is an administrator.
										return (dramatic ? Promise.reject(new Error("Insufficient authority on target record.")) : -1);
									}
									return 0;
								}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; } }));
							}
							if (check_recursion) {
								mchecks.push(security.checkRecursiveDocumentDepth(sfields[pkk]['target_class'], data[pkk], pkk, {}, null).then(function (reclev) {
									// console.log("Depth:", ((reclev >= 0 && reclev < 0xFFFF) ? 0 : -1));
									return ((reclev >= 0 && reclev < 0xFFFF) ? 0 : (dramatic ? Promise.reject(new Error("References too deep.")) : -1));
								}, function(err) { if (dramatic) { return Promise.reject(err); } else { return -1; } }));
							}
							return Promise.all(mchecks).then(function (checkresults) {
								var rv = 0;
								// console.log("Authority and depth for", pkk, ":", checkresults, ".");
								checkresults.forEach(function (currentValue, index, array) { if (currentValue < 0) rv = -1; });
								return rv;
							}, function (err) { return Promise.reject(err); });
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
					return Promise.reject(new Error("Attempting to write to undocumented field."));
				} else {
					// console.log("Attempting to write to an undocumented field.");
					return Promise.resolve(-1);
				}
			}
		}
	}
	// Now we use the traditional validation (roughly equivalent to the feathers-validator but implemented internally).
	// This runs on all schema items (including those absent from the input object) so as to be able to check for required values,
	// except when parsing a patch or when the update is overlay-only and the field is not in the overlay.
	for (var skey in sfields) {
		if ('validation' in sfields[skey] && ((operation != 'patch' && !(overlay_only && !(('overlay' in overrides) ? overrides['overlay'] : ('overlay' in sfields[skey] && sfields[skey]['overlay'])))) || skey in data)) {
			if ((('allownull' in sfields[skey] && sfields[skey]['allownull']) ||
					('allow_null' in sfields[skey] && sfields[skey]['allow_null'])) &&
					skey in data && data[skey] == null) {
				// console.log("Null value allowed.");
			} else if (validateText(sfields[skey]['validation'], ((skey in data) ? data[skey] : null)) < 0) {
				// console.log("Validation failed on", skey, "on rule", sfields[skey]['validation'], ".");
				if (dramatic) return Promise.reject(new Error("Validation failed on " + skey + " on rule " + sfields[skey]['validation'] + "."));
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
	}, function (err) { return Promise.reject(err); });
}

function checkTypes(schema, data, exclusive, connections, target_user, dramatic) {
	return checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, '', null, 0, {});
}

function coerceNumericToIntegerPatch(schema, data) {
	// This converts any text values that ought to be numbers into numbers.
	var output = {};
	for (var pkey in data) {
		if (pkey in schema) {
			if ('type' in schema[pkey] && typeof(data[pkey]) == 'string' && schema[pkey].type == 'number') {
				var tmp = parseInt(data[pkey]);
				if (typeof(tmp) == 'number') {
					output[pkey] = tmp;
				} else {
					output[pkey] = null;
				}
			} else {
				output[pkey] = data[pkey];
			}
		} else {
			output[pkey] = data[pkey];
		}
	}
	return output;
}

function splitPatch(schema, data) {
	// This takes a create/patch request and breaks it into base changes and overlay changes.
	// This runs after the type check.
	var baseData = {};
	var overlayData = {};
	var ci;
	for (ci in data) {
		if (ci in schema) {
			if ('overlay' in schema[ci] && schema[ci].overlay) {
				overlayData[ci] = data[ci];
			} else {
				baseData[ci] = data[ci];
			}
		}
	}
	return {data, baseData, overlayData};		
}

function mergePatch(baseData, overlayData) {
	// This takes a create/patch request and breaks it into base changes and overlay changes.
	// This runs after the type check.
	var data = {};
	var ci;
	for (ci in baseData) {
		data[ci] = baseData[ci];
	}
	for (ci in overlayData) {
		data[ci] = overlayData[ci];
	}
	return data;
}

function mergeSchemedPatch(schema, baseData, overlayData) {
	// This takes a create/patch request and breaks it into base changes and overlay changes.
	// This runs after the type check.
	var data = {};
	var ci;
	for (ci in baseData) {
		data[ci] = baseData[ci];
	}
	for (ci in overlayData) {
		if (ci in schema) {
			if ('overlay' in schema[ci] && schema[ci].overlay) {
				data[ci] = overlayData[ci];
			}
		}
	}
	return {data, baseData, overlayData};	
}

function mergeSchemedPatchInPlace(schema, baseData, overlayData) {
	// This takes a create/patch request and breaks it into base changes and overlay changes.
	// This runs after the type check.
	var data = baseData;
	var ci;
	for (ci in overlayData) {
		if (ci in schema) {
			if ('overlay' in schema[ci] && schema[ci].overlay) {
				data[ci] = overlayData[ci];
			}
		}
	}
	return baseData;		
}

function populateHierarchy(base, path, value) {
	if (path.length <= 0) {
	} else if (path.length == 1) {
		base[path[0]] = value;
	} else {
		if (!(path[0] in base)) base[path[0]] = {};
		populateHierarchy(base[path[0]], path.slice(1), value);
	}
	return 0;
}

function splitSubmodelData(schema, data) {
	// This takes an object containing flattened data and extracts fields into subobjects according to the model.
	// This runs before the type check.
	var security = null;
	if (this && 'models' in this && 'app' in this) security = this;
	var fieldMap = null;
	if (security) fieldMap = security.splitSubmodel(schema, '_').fields;
	else fieldMap = splitSubmodel(schema, '_').fields;
	var output = {};
	var ci;
	for (ci in data) {
		if (ci in fieldMap) {
			populateHierarchy(output, fieldMap[ci], data[ci]);
		}
	}
	return output;
}

function flattenHierarchy(dest, prefix, delimiter, current) {
	if (current == null) {
	} else if (current instanceof Object) {
		var tc;
		for (tc in current) {
			flattenHierarchy(dest, (prefix ? prefix + delimiter + tc : tc), delimiter, current[tc]);
		}
	} else if (prefix) {
		dest[prefix] = current;
	}
	return 0;
}

function mergeSubmodelData(schema, data) {
	// This takes an object containing data according to the provided model and flattens the contents of subobjects into the top-level dictionary.
	// The operation presupposes non-conflicting names.
	var output = {};
	flattenHierarchy(output, null, '_', data);
	return output;
}

function splitSubmodel(schema, delimiter) {
	// This generates flat names for nested entities and maps them to nested entity paths.
	// Like {'address_street_address': ['address', 'street_address']}.
	// This takes just the field map for schema, not the full schema structure.
	var security = null;
	if (this && 'models' in this && 'app' in this) {
		security = this;
	}
	var output = {};
	var si;
	if ('fields' in schema) {
		for (si in schema.fields) {
			if ('submodel' in schema.fields[si] || 'submodel_inline' in schema.fields[si]) {
				var tschema = {};
				if ('submodel_inline' in schema.fields[si] && 'fields' in schema.fields[si].submodel_inline) {
					if (security)
						tschema = security.splitSubmodel(schema.fields[si]['submodel_inline'], delimiter);
					else
						tschema = splitSubmodel(schema.fields[si]['submodel_inline'], delimiter);
				} else if (security && 'submodel' in schema.fields[si] && 'fields' in security.models[schema.fields[si].submodel]) {
					tschema = security.splitSubmodel(security.models[schema.fields[si].submodel], delimiter);
				}
				var tsi;
				for (tsi in tschema.fields) {
					// Construct the flat name by joining the incoming name to the name of the current key.
					// And construct the path by joining the incoming array to an array containing the name of the current key.
					if ((si + delimiter + tsi) in output) console.log("Schema conflict.");
					else output[si + delimiter + tsi] = ([si]).concat(tschema.fields[tsi]);
				}
			} else {
				if (si in output) console.log("Schema conflict.");
				else output[si] = [si];
			}
		}
	}
	return {fields: output};
}

function convertSubmodelFlattenByMap(schema, mapping) {
	// This takes an inlined schema only.
	// This takes the output from splitSubmodel as mapping.
	// TODO: Rebase column names in easyCompute properties.
	var output = {fields: {}};
	var entry;
	for (entry in mapping.fields) {
		var current_node = {'submodel_inline': schema};
		var current_node_name;
		var err = 0;
		mapping.fields[entry].forEach(function (current_node_name, mei, mea) {
			if ('submodel_inline' in current_node &&
					'fields' in current_node['submodel_inline'] &&
					current_node_name in current_node['submodel_inline']['fields']) {
				current_node = current_node['submodel_inline']['fields'][current_node_name];
			} else {
				err = 1;
			}
		});
		if (!err) output.fields[entry] = current_node;
	}
	return output;
}

function defaultValueDataFromSchema(schema, optionals) {
	// This generates a record containing default or space-filler values.
	// If there are no relations and no strings that must be longer than zero characters,
	// it might give a record that would pass the type check.
	var security = null;
	if (this && 'models' in this && 'app' in this) security = this;
	var output = {};
	var si;
	for (si in schema.fields) {
		if (security && 'submodel' in schema.fields[si]) {
				output[si] = defaultValueDataFromSchema(security.models[schema.fields[si].submodel], optionals);
		} else if ('submodel_inline' in schema.fields[si]) {
			if (security)
				output[si] = security.defaultValueDataFromSchema(schema[si].submodel_inline, 1);
			else
				output[si] = defaultValueDataFromSchema(schema[si].submodel_inline, 1);
		} else if ('type' in schema.fields[si] &&
				(!('is_primary_key' in schema.fields[si] && schema.fields[si].is_primary_key)) &&
				(!('target_class' in schema.fields[si] && schema.fields[si].target_class.length > 0)) &&
				(!('is_user_writable' in schema.fields[si] && !(schema.fields[si].is_user_writable))) &&
				(!('computed' in schema.fields[si] && schema.fields[si].computed))) {
			// We need to figure out the rules for the data to be generated.
			var vmin = 0;
			var vmax = 0;
			var vrequired = 0;
			var vrules = [];
			if ('validation' in schema.fields[si]) vrules = tokenizeValidationRule(schema.fields[si].validation);
			vrules.forEach(function (rule, r_ind, r_arr) {
				if (rule.length > 1) {
					if (rule[0] == 'min') {
						vmin = parseInt(rule[1]);
					} else if (rule[0] == 'max') {
						vmax = parseInt(rule[1]);
					}
				} else if (rule.length > 0) {
					if (rule[0] == 'required') {
						vrequired = 1;
					}
				}
			});
			// Now we generate data according to those rules.
			if (vrequired || optionals) {
				if (schema.fields[si].type == 'string') {
					var scnt = 0;
					var sspace = "";
					while (scnt < vmin) {
						sspace += (scnt % 10).toString();
						scnt++;
					}
					output[si] = sspace;
				} else if (schema.fields[si].type == 'number') {
					output[si] = vmin;
				}
			}
		}
	}
	return output;
}

function dictDiff(i0, i1, proportional) {
	var output = {};
	for (fn in i0) {
		if (fn in i1) {
			if (proportional) {
				if (i0[fn] > 0) {
					output[fn] = (i1[fn] - i0[fn]) / i0[fn];
				} else {
					output[fn] = NaN;
				}
			} else {
				output[fn] = (i1[fn] - i0[fn]);
			}
		}
	}
	return output;
}

function schemaGetQuantitativeNames(schema, inferQuantityFields, ignoreComputedFields) {
	var names = [];
	var fname;
	for (fname in schema.fields) {
		var field = schema.fields[fname];
		if ('type' in field && field.type == 'number' &&
				(('quantitative' in field && field.quantitative) ||
				(inferQuantityFields &&
				!('is_primary_key' in field && field.is_primary_key) &&
				!('target_class' in field && field.target_class))) &&
				!('label' in field && field.label) &&
				!(ignoreComputedFields && 'computed' in field && field.computed)) {
			names.push(fname);
		}
	};
	return names;
}

function vectorTallyWithSchemaArrays(schema, data, names, invertFirst) {
	// This expects flat schema (with no submodels).
	// Data is an array of records.
	// invertFirst reverses the sign of the first row, allowing this function to be used for differencing.
	// Make space for the total and the count of the input values for each column.
	var value_totals = new Array(names.length);
	value_totals.fill(0);
	var item_totals = new Array(names.length);
	item_totals.fill(0);
	// Add the value to the one vector and increment the value count in the other.
	// TODO: Consider using parallel.js here.
	data.forEach(function (row, r_ind, r_arr) {
		names.forEach(function (n, n_ind, n_arr) {
			if (n in row && typeof(row[n]) == 'number') {
				if (invertFirst && r_ind == 0) value_totals[n_ind] -= row[n];
				else value_totals[n_ind] += row[n];
				item_totals[n_ind] += 1;
			}
		});
	});
	return {names: names, totals: value_totals, counts: item_totals};
}

function vectorAverageWithSchemaByName(schema, data, names) {
	// console.log("Names.");
	// console.log(names);
	var iv = vectorTallyWithSchemaArrays(schema, data, names, 0);
	// console.log("Values.");
	// console.log(iv);
	// var output_a = new Array(iv.names.length);
	var output_d = {};
	iv.names.forEach( function (n, ind, n_arr) {
		if (iv.counts[ind] > 0) {
			// output_a[ind] = iv.totals[ind] / iv.counts[ind];
			output_d[n] = iv.totals[ind] / iv.counts[ind];
		} else {
			// output_a[ind] = null;
			output_d[n] = null;
		}
	});
	return output_d;
}

function padZero(val, padsize) {
	var tv = val.toString();
	var zcount = padsize - tv.length;
	var tp = "";
	var ocount = 0;
	while (ocount < zcount) {
		tp += "0";
		ocount++;
	}
	return tp + tv;
}

function generateFormCrude(document, ischema, includeAll, includeVisible, includeLabelled, preferExplanations, breaks) {
	var fschema_map = splitSubmodel(ischema, "_");
	var fschema = convertSubmodelFlattenByMap(ischema, fschema_map);
	var output = [];
	var fl = fschema.fields;
	var fname;
	for (fname in fl) {
		var fe = fl[fname];
		if ((includeLabelled  &&
				(('human_name' in fe && fe['human_name']) ||
				('special_explanation' in fe && fe['human_name']) ||
				('label' in fe && fe['label']))) ||
				(includeVisible && 'visible' in fe && fe['visible']) ||
				includeAll) {
			// We determine that this field somehow qualifies to appear on the form.
			var tl = null;
			if ('label' in fe && fe['label']) {
				// If this is just a stand-alone label, it is a p tag.
				if ('form_tag' in fe && fe['form_tag'])
					tl = document.createElement(fe['form_tag']);
				else
					tl = document.createElement("p");
				tl.setAttribute("name", fname);
			} else {
				// If we are putting a label on an input field, it is a label tag.
				tl = document.createElement("label");
			}
			if (preferExplanations && 'special_explanation' in fe &&
					fe['special_explanation']) {
				var tt = document.createTextNode(fe['special_explanation']);
				tl.appendChild(tt);
			} else if ('human_name' in fe && fe['human_name']) {
				var tt = document.createTextNode(fe['human_name']);
				tl.appendChild(tt);
			}
			if (breaks) {
				var tb = document.createElement("br");
				tl.appendChild(tb);
			}
			if (!('label' in fe && fe['label'])) {
				if ('type' in fe && fe['type']) {
					// Parse the validation rules, if present.
					var tmin = null;
					var tmax = null;
					var treq = null;
					var tstep = null;
					var twrite = null;
					var tcomputed = null;
					if ('validation' in fe) {
						var rules = tokenizeValidationRule(fe['validation']);
						rules.forEach(function(rval, r_ind, r_arr) {
							if (rval.length > 1) {
								if (rval[0] == 'min') {
									tmin = rval[1];
								} else if (rval[0] == 'max') {
									tmax = rval[1];
								} else if (rval[0] == 'step') {
									tstep = rval[1];
								}
							} else if (rval.length > 0) {
								if (rval[0] == 'required') {
									treq = 1;
								}
							}
						});
					}
					if ('computed' in fe) {
						if (fe['computed']) {
							tcomputed = 1;
						} else {
							tcomputed = 0;
						}
					}
					if ('is_user_writable' in fe) {
						if (fe['is_user_writable']) {
							twrite = 1;
						} else {
							twrite = 0;
						}
					}
					var tf = null;
					if (fe['type'] == 'object') {
						// We only accept dates right now.
						if ('stype' in fe && fe['stype'] == 'date') {
							// Insert a date selector.
							tf = document.createElement("input");
							tf.setAttribute("type", "date");
						}
					} else if (fe['type'] == 'number') {
						tf = document.createElement("input");
						tf.setAttribute("type", "number");
						if (tmin != null)
							tf.setAttribute("min", tmin.toString());
						if (tmax != null)
							tf.setAttribute("max", tmax.toString());
						if (tstep != null)
							tf.setAttribute("step", tstep.toString());
					} else if (fe['type'] == 'string') {
						tf = document.createElement("input");
						tf.setAttribute("type", "number");
						if (tmin != null)
							tf.setAttribute("min", tmin.toString());
						if (tmax != null)
							tf.setAttribute("max", tmax.toString());
					}
					if (tf != null) {
						if (treq) tf.setAttribute("required", 1);
						tf.setAttribute("name", fname);
						if ((twrite != null && !twrite) ||
								(tcomputed != null && tcomputed)) {
							tf.setAttribute("readonly", "1");
						}
						tl.appendChild(tf);
						if (breaks) {
							var tb = document.createElement("br");
							tl.appendChild(tb);
						}
					}
				}
			}
			output.push(tl);
		}
	}
	return output;
}

function numberCheck(str) {
	if (typeof(str) == 'number') return 1;
	if (str !== null && str.length > 0 && !isNaN(str))
		return 1;
	return 0;
}

function extractFormData(schema, fschema, tform) {
	// This requires flattened schema and normal schema.
	// The caching improves performance.
	var tfs = fschema.fields;
	var fname;
	var tdata = {};
	for (fname in tfs) {
		var tf = tfs[fname];
		if (!('label' in tf && tf['label']) && !('computed' in tf && tf['computed']) &&
				!('is_user_writable' in tf && !tf['is_user_writable'])) {
			if (fname in tform.elements && 'value' in tform.elements[fname]) {
				var tv = tform.elements[fname].value;
				var tvc = tv;
				// console.log(tf);
				if ('type' in tf) {
					if (tf.type == 'number' && typeof(tv) != 'number') {
						if (numberCheck(tv))
							tvc = new Number(tv);
						else
							tvc = null;
					}
				}
				if (tvc != null)
					tdata[fname] = tvc;
			}
		}
	}
	return splitSubmodelData(schema, tdata);
}

function filterFormData(schema, data) {
	// This filters out unwritable fields.
	// If submodel is set, this will try to find schema in this, so be careful.
	if (data == null) return null;
	var security = null;
	var tfs = schema.fields;
	var fname;
	var tdata = {};
	for (fname in tfs) {
		var tf = tfs[fname];
		if (!('label' in tf && tf['label']) && !('computed' in tf && tf['computed']) &&
				!('is_user_writable' in tf && !tf['is_user_writable'])) {
			if (fname in data) {
				if (security && 'models' in security && 'app' in security && 'submodel' in tf && tf['submodel'] != null) {
					if (tf['submodel'] in security.models) {
						tdata[fname] = filterFormData(security.models[tf['submodel']], data[fname]);
					}
				} else if ('submodel_inline' in tf && tf['submodel_inline'] != null) {
					tdata[fname] = filterFormData(tf['submodel_inline'], data[fname]);
				} else {
					tdata[fname] = data[fname];
				}
			}
		}
	}
	return tdata;
}

function vectorDifferenceWithSchemaByName(schema, data, names) {
	var iv = vectorTallyWithSchemaArrays(schema, data, names, 1);
	// var output_a = new Array(iv.names.length);
	var output_d = {};
	iv.names.forEach( function (n, ind, n_arr) {
		if (iv.counts[ind] > 0) {
			// output_a[ind] = iv.totals[ind] / iv.counts[ind];
			output_d[n] = iv.totals[ind];
		} else {
			// output_a[ind] = null;
			output_d[n] = null;
		}
	});
	return output_d;
}

function easyCompute(schema, data, formula, allow_recursion, fallback_data) {
	// TODO: Figure out errors.
	if ('value' in formula) {
		return formula['value'];
	} else if ('column' in formula) {
		// TODO: Add support for relative paths to submodels and supermodels.
		if (formula['column'] in data) {
			return data[formula['column']];
		} else if (fallback_data && formula['column'] in fallback_data) {
			return fallback_data[formula['column']];
		} else if (allow_recursion && 'fields' in schema && formula['column'] in schema.fields &&
				'computed' in schema.fields[formula['column']] &&
				schema.fields[formula['column']]['computed'] &&
				'easy_computation' in schema.fields[formula['column']] &&
				schema.fields[formula['column']]['easy_computation'] != null) {
			return easyCompute(schema, data, schema.fields[formula['column']]['easy_computation'], allow_recursion, fallback_data);
		} else if ('fields' in schema && formula['column'] in schema.fields) {
			// It is not a bad reference, just a missing field.
		} else {
			console.log("Bad reference " + formula['column'] + " in formula.");
		}
	} else if ('sum' in formula) {
		var acc = 0;
		formula['sum'].forEach(function (addend, a_ind, a_arr) {
			acc += easyCompute(schema, data, addend, allow_recursion, fallback_data);
		});
		return acc;
	} else if ('product' in formula) {
		var acc = 1;
		formula['product'].forEach(function (addend, a_ind, a_arr) {
			acc *= easyCompute(schema, data, addend, allow_recursion, fallback_data);
		});
		return acc;
	} else if ('pow' in formula) {
		var pbase = easyCompute(schema, data, formula['pow'][0], allow_recursion, fallback_data);
		var pexp = easyCompute(schema, data, formula['pow'][1], allow_recursion, fallback_data);
		if (formula['pow'].length > 1 && (pexp >= 0 || pbase > 0)) {
			return Math.pow(pbase, pexp);
		}
	}
	return 0;
}

function coerceValues(schema, data) {
	var security = null;
	if (this && 'models' in this && 'app' in this) security = this;
	var fname;
	var output = {};
	for (fname in schema.fields) {
		var field = schema.fields[fname];
		if (fname in data) {
			if (data[fname] != null) {
				if ('type' in field && (typeof(data[fname]) != field['type'] ||
						('instanceof' in field && field['instanceof'] != null && !(data[fname] instanceof field['instanceof'])))) {
					if ('instanceof' in field && field['instanceof'] != null) {
						// console.log("instanceof conversion on " + fname + ".");
						try {
							output[fname] = new field['instanceof'](data[fname]);
						} catch (err) {
							console.log(err);
							output[fname] = null;
						}
					} else {
						// console.log("Flat type conversion on " + fname + ".");
						try {
							if (field['type'] == 'number') {
								output[fname] = new Number(data[fname]);
							} else if (field['type'] == 'string') {
								output[fname] = new String(data[fname]);
							} else {
								output[fname] = null;
							}
						} catch (err) {
							output[fname] = null;
						}
					}
				} else {
					// If the field is provided, we copy/process it.
					// console.log("Type match on " + fname + ".");
					if (security && 'submodel' in field && typeof(field['submodel']) == 'string' && field['submodel'].length > 0 &&
							field['submodel'] in security.models) {
						output[fname] = security.coerceValues(security.models[field['submodel']], data[fname]);
					} else if ('submodel_inline' in field && field['submodel_inline'] instanceof Object) {
						if (security)
							output[fname] = security.coerceValues(field['submodel_inline'], data[fname]);
						else
							output[fname] = coerceValues(field['submodel_inline'], data[fname]);
					} else {
						output[fname] = data[fname];
					}
				}
			} else {
				// console.log("Null on " + fname + ".");
				output[fname] = data[fname];
			}
		}
	}
	return output;		
}

function includeEasyComputedValues(schema, data) {
	var security = null;
	if (this && 'models' in this && 'app' in this) security = this;
	var fname;
	var output = {};
	for (fname in schema.fields) {
		var field = schema.fields[fname];
		if ('type' in field && field.type == 'number' &&
				'computed' in field && field.computed &&
				'easy_computation' in field) {
			// If the field is computed, we generate it.
			output[fname] = easyCompute(schema, data, field['easy_computation'], 1, output);
		} else if (fname in data) {
			// If the field is provided, we copy/process it.
			if (security && 'submodel' in field && typeof(field['submodel']) == 'string' && field['submodel'].length > 0 &&
					field['submodel'] in security.models) {
				output[fname] = security.includeEasyComputedValues(security.models[field['submodel']], data[fname]);
			} else if ('submodel_inline' in field && field['submodel_inline'] instanceof Object) {
				if (security)
					output[fname] = security.includeEasyComputedValues(field['submodel_inline'], data[fname]);
				else
					output[fname] = includeEasyComputedValues(field['submodel_inline'], data[fname]);
			} else {
				output[fname] = data[fname];
			}
		}
	};
	return output;
}

function generateValidatorSchema(schema) {
	// This converts the internal validator schema/rules to something compatible with feathers-validator.
	var rv = {};
	for (var tkey in schema) {
		rv[tkey] = schema[tkey].validation;
	}
	return rv;
}

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
	//			fields: {
	//				_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
	//				owner_id: {type: 'string', validation: 'max:255|alpha_dash', is_user_writable: 1, target_class: 'partners', target_authority: 2},
	// 				created_at: {type: 'date', validation: '', is_user_writable: 0},
	// 				created_by: {type: 'date', validation: '', is_user_writable: 0}
	//			}
	//		},
	//		'partners': {
	//			fields: {
	//				_id: {type: 'string', validation: 'max:255|alpha_dash', is_primary_key: 1, is_user_writable: 0},
	//	 			name: {type: 'string', validation: 'max:65535', is_user_writable: 1},
	// 				created_at: {type: 'date', validation: '', is_user_writable: 0},
	// 				created_by: {type: 'date', validation: '', is_user_writable: 0}
	//			}
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
	dateConvertToUTC: function(iv) {
		// This converts a date with nominal values in the active time zone to one with nominal values in UTC.
		return new Date(iv.getUTCFullYear(), iv.getUTCMonth(), iv.getUTCDate(), iv.getUTCHours(), iv.getUTCMinutes(), iv.getUTCSeconds(), iv.getUTCMilliseconds());
	},
	tokenizeValidationRule: tokenizeValidationRule,
	validateText: validateText,
	checkTypesNested: checkTypesNested,
	checkTypes: checkTypes,
	checkTypesCreate: function(schema, data, exclusive, connections, target_user, dramatic, base_record) {
		// This allows setting administrator-only values to their defaults.
		// It ignores base_record.
		return this.checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, 'create', null, 0, {});
	},
	checkTypesUpdate: function(schema, data, exclusive, connections, target_user, dramatic, base_record) {
		// This accepts a base record and blocks changes to administrator-only values.
		return this.checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, 'update', base_record, 0, {});
	},
	checkTypesPatch: function(schema, data, exclusive, connections, target_user, dramatic, base_record) {
		// This accepts a base record and blocks patch entries for administrator-only values.
		return this.checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, 'patch', base_record, 0, {});
	},
	checkTypesUpdateOverlay: function(schema, data, exclusive, connections, target_user, dramatic, base_record) {
		// This accepts a base record and blocks changes to administrator-only values.
		return this.checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, 'update', base_record, 1, {});
	},
	checkTypesPatchOverlay: function(schema, data, exclusive, connections, target_user, dramatic, base_record) {
		// This accepts a base record and blocks patch entries for administrator-only values.
		return this.checkTypesNested(schema, data, exclusive, connections, target_user, dramatic, 'patch', base_record, 1, {});
	},
	coerceNumericToIntegerPatch: coerceNumericToIntegerPatch,
	splitPatch: splitPatch,
	mergePatch: mergePatch,
	mergeSchemedPatch: mergeSchemedPatch,
	mergeSchemedPatchInPlace: mergeSchemedPatchInPlace,
	populateHierarchy: populateHierarchy,
	splitSubmodelData: splitSubmodelData,
	flattenHierarchy: flattenHierarchy,
	mergeSubmodelData: mergeSubmodelData,
	mergeSubmodelDataHook: function(target_class, hook) {
		// This is for use as an after hook for get requests.
		if ('flatForm' in hook.params && hook.params.flatForm) {
			var tmp = mergeSubmodelData(hook.app.security.data_schema[target_class].fields, hook.data);
			hook.data = tmp;
		}
		return Promise.resolve(hook);
	},
	splitSubmodelResultHook: convertSubmodelFlattenByMap,
	inlineSchema: inlineSchema,
	defaultValueDataFromSchema: defaultValueDataFromSchema,
	dictDiff: dictDiff,
	schemaGetQuantitativeNames: schemaGetQuantitativeNames,
	vectorTallyWithSchemaArrays: vectorTallyWithSchemaArrays,
	vectorAverageWithSchemaByName: vectorAverageWithSchemaByName,
	padZero: padZero,
	generateFormCrude: generateFormCrude,
	numberCheck: numberCheck,
	extractFormData: extractFormData,
	filterFormData: filterFormData,
	vectorAverageWithSchema: function(schema, data, inferQuantityFields, ignoreComputedFields) {
		// If inferQuantityFields is set, this will use any field typed as a number and not used as a direct or referenced primary key as a quantitative value.
		var ischema = this.inlineSchema(schema);
		var fschema_map = this.splitSubmodel(schema, "_");
		var fschema = this.convertSubmodelFlattenByMap(ischema, fschema_map);
		var names = this.schemaGetQuantitativeNames(fschema, inferQuantityFields, ignoreComputedFields);
		var fdata = [];
		var security = this;
		data.forEach(function (rec, r_ind, r_arr) {
			fdata.push(security.mergeSubmodelData(fschema, rec));
		});
		var averages = this.vectorAverageWithSchemaByName(fschema, fdata, names);
		return this.splitSubmodelData(schema, averages);
	},
	vectorDifferenceWithSchemaByName: vectorDifferenceWithSchemaByName,
	vectorDifferenceWithSchema: function(schema, data, inferQuantityFields, ignoreComputedFields) {
		// If inferQuantityFields is set, this will use any field typed as a number and not used as a direct or referenced primary key as a quantitative value.
		var ischema = this.inlineSchema(schema);
		var fschema_map = this.splitSubmodel(schema, "_");
		var fschema = this.convertSubmodelFlattenByMap(ischema, fschema_map);
		var names = this.schemaGetQuantitativeNames(fschema, inferQuantityFields, ignoreComputedFields);
		var fdata = [];
		var security = this;
		data.forEach(function (rec, r_ind, r_arr) {
			fdata.push(security.mergeSubmodelData(fschema, rec));
		});
		var differences = this.vectorDifferenceWithSchemaByName(fschema, fdata, names);
		return this.splitSubmodelData(schema, differences);
	},
	easyCompute: easyCompute,
	coerceValues: coerceValues,
	includeEasyComputedValues: includeEasyComputedValues,
	generateValidatorSchema: generateValidatorSchema,
	checkAuthorityEntry: function(data, target_user, dramatic) {
		var target_class = null;
		if ('target_class' in data && this.validateText('max:255|alpha_dash', data['target_class']) >= 0 &&
				data['target_class'] in this.data_schema && this.data_schema[data['target_class']] instanceof Object &&
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
			return this.checkTypesCreate({fields: dyn_schema}, data, 1, 1, target_user, dramatic, null);
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
					if (tmpv < 0) return Promise.resolve(tmpv);
					return Promise.resolve(tmpv + 1);
				}, function(err) {console.error('Query error in checkSaleDocumentDepth.', err); return Promise.resolve(-1);});
			}
			return Promise.resolve(0);
		}, function(err) {console.error('Query error in checkSaleDocumentDepth.', err); return Promise.resolve(-1);});
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
						if ('mask' in currentValue) {
							rv.push({'target_class': currentValue['class'], 'target_id': curr_entity[currentValue['key']], 'mask': currentValue['mask']});
						} else {
							rv.push({'target_class': currentValue['class'], 'target_id': curr_entity[currentValue['key']]});
						}
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
		var security = this;
		// We also want to be sure that the record actually exists.
		var p0 = this.app.service(target_class).get(target_id).then(function (rec) { return 0; },
		function (err) { return Promise.reject(err); });
		// let searchParams = {user: target_user, target: target_id, '$sort': {destroyed_at: -1}};
		var searchParams = {'user_id': target_user, 'target_class': target_class, 'target_id': target_id, destroy_date: null};  // TODO: Change to let.
		// console.log("Query.", searchParams);
		var qresult1p = this.app.service('authorities').find({query: searchParams});  // TODO: Change to let.
		// Iterate through all relevant authority records for this uuid pair and find maximum privilege.
		var p1 = qresult1p.then(function (curr_auth) {
			var maxacc1 = 0;  // TODO: Change to let.
			// If the target_class is user and the id matches and there is a defined self-access level, add it.
			if (target_class == 'users' && 'user_self_access' in security && typeof(security.user_self_access) == 'number' && target_id == target_user)
				maxacc1 |= security.user_self_access;
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
				parent_levels.forEach(function (currentValue, index, array) {
					var tmp = currentValue;
					tmp &= ~1; // We drop the lowest bit, as it is non-transitive.
					if ('mask' in target_parents[index]) tmp &= target_parents[index].mask; // We apply a transit mask.
					parent_max_acc |= (currentValue & ~1); // We add the privilege for this parent to the accumulator.
				});
				return parent_max_acc;
			}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
		}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
		// Now we combine the results of the immediate and parent privilege computations.
		var acc_join = Promise.all([p0, p1, p2]);
		return acc_join.then(function (parent_levels) {
			var parent_max_acc = 0; // TODO: Change to let.
			parent_levels.forEach(function (currentValue, index, array) { parent_max_acc |= currentValue; });
			// console.log("Access level:", parent_max_acc);
			return parent_max_acc;
		}, function(err) {console.error('Query error in accessLevelSlow.', err); return 0;});
	},
	userIsAdministrator: function(target_user) {
		// console.log("Checking whether", target_user, "is administrator.");
		if (target_user == null) return Promise.resolve(1);
		var security = this;
		// If there is an administrator flag, check the user to see whether he has it.
		if ('user_administrator_flag_name' in security && typeof(security.user_administrator_flag_name) == 'string') {
			var uresult1p = security.app.service('users').get(target_user, {query: (('users' in security.data_schema && 'overlay_name' in security.data_schema['users']) ? {'$overlay': 1} : {})});
			return uresult1p.then(
				function (uv) {
					if (security.user_administrator_flag_name in uv && uv[security.user_administrator_flag_name]) {
						// console.log("Yes.");
						return Promise.resolve(1);
					}
					// console.log("No flag so no.");
					return Promise.resolve(0);
				},
				function (err) {
					return Promise.reject(err);
				}
			);
		}
		// console.log("No schema so no.");
		return Promise.resolve(0);
	},
	accessLevelSlowWithUser: function(target_user, target_class, target_id) {
		var pp = [this.userIsAdministrator(target_user),
		this.accessLevelSlow(target_user, target_class, target_id),
		this.app.service(target_class).get(target_id)];
		// Note that we must convert the flag from userIsAdministrator into a set of privilege flags (0xFF).
		return Promise.all(pp).then(
			function (rv) { return (rv[0] ? 0xFF : 0) | rv[1]; },
			function (err) { return Promise.reject(err); }
		);
	},
	userCanRead: function(target_user, target_class, target_id) {
		if (target_user == null) return Promise.resolve(1);
		return (this.accessLevelSlowWithUser(target_user, target_class, target_id).then(function (x) {
			return ((x & 0x3) ? 1 : 0);
		}, function (err) {return Promise.resolve(0);}));
	},
	userCanWrite: function(target_user, target_class, target_id) {
		if (target_user == null) return Promise.resolve(1);
		return (this.accessLevelSlowWithUser(target_user, target_class, target_id).then(function (x) {
			return (x & 6) == 6;
		}, function (err) {return Promise.resolve(0);}));
	},
	userCanListChildren: function(target_user, target_class, target_id) {
		if (target_user == null) return Promise.resolve(1);
		return (this.accessLevelSlowWithUser(target_user, target_class, target_id).then(function (x) {
			return ((x & 0x2) ? 1 : 0);
		}, function (err) {return Promise.resolve(0);}));
	},
	userCanAddChildren: function (target_user, target_class, target_id) {
		if (target_user == null) return Promise.resolve(1);
		return this.userCanWrite(target_user, target_class, target_id);
	},
	hookRestrictToAdministrator: function (hook) {
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		var security = hook.app.security;
		if ('user' in hook.params && security.id_name in hook.params.user) {
			// Check for administrative access.
			return security.userIsAdministrator(hook.params.user[security.id_name]).then( function (is_admin) {
				if (is_admin) return hook;
				return Promise.reject(new Error("Only an administrator can do that."));
			}, function (err) { return Promise.reject(err); });
		}
		return Promise.reject(new Error("User data is not attached to the hook, probably as a result of a configuration problem."));
	},
	hookRestrictToOwner: function (params, hook) {
		// This is like the one from auth, but it respects administrator flags.
		// It requires hook.params.user to be populated.
		// console.log("Hook parameters.");
		// console.log(hook.params);
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		var security = hook.app.security;
		var thisC = this;
		if ('user' in hook.params) {
			// Check for administrative access.
			// console.log("Checking administrativeness.");
			return security.userIsAdministrator(hook.params.user[security.id_name]).then( function (is_admin) {
				// console.log(is_admin.toString() + ".");
				if (is_admin) return hook;
				else {
					if ('ownerField' in params) {
						// Fetch the record and check the owner field.
						// console.log("Fetching target.");
						return thisC.get(hook.id).then(function (rec) {
							// console.log(rec);
							if (params['ownerField'] in rec && hook.params.user[hook.app.security.id_name] == rec[params['ownerField']]) {
								// console.log("Access granted.");
								return hook;
							}
							return Promise.reject(new Error("Only the owner of the record can access it."));
						});
					}
				}
				return Promise.reject(new Error("Only the owner of the record can access it."));
			}, function (err) { return Promise.reject(err); });
		}
		return Promise.reject(new Error("User data is not attached to the hook, probably as a result of a configuration problem."));
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
					fetchers.push(Promise.resolve({}));
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
			return hook.app.security.checkTypesCreate({fields: hook.app.security.pickPathFieldsBase}, hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1, null).then(
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
		var timestamp = hookTimestamp(hook);
		return this.patch(hook.id, { destroyed_at: timestamp, destroyed_by: hook.params.user[hook.app.security.id_name] }).then(function(data) {
			// Set the result from `patch` as the method call result
			hook.result = data;
			// Always return the hook or `undefined`
			return hook;
		});
	},
	hookSoftDestroyHook: function () {
		// TODO: Find a way to expose this as a static function.
		var security = this;
		return function (hook) {
			return security.hookSoftDestroy.apply(this, [hook]);
		};
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
		if (!('created_at' in hook.data)) hook.data.created_at = hookTimestamp(hook);
		if ('user' in hook.params && !('created_by' in hook.data)) hook.data.created_by = hook.params.user[hook.app.security.id_name];
		// console.log("Added creation information.");
		return hook;
	},
	hookAddDestructionSpaceInPlaceBefore: function(hook) {
		// console.log("Adding creation information.");
		hook.data.destroyed_at = null;
		if ('user' in hook.params && !('destroyed_by' in hook.data)) hook.data.destroyed_by = null;
		// console.log("Added creation information.");
		return hook;
	},
	hookAddDestructionInformationInPlaceBefore: function(hook) {
		// We use this only when creating an already destroyed record.
		// console.log("Adding creation information.");
		hook.data.destroyed_at = hookTimestamp(hook);
		if ('user' in hook.params && !('destroyed_by' in hook.data)) hook.data.destroyed_by = hook.params.user[hook.app.security.id_name];
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
	checkRulesCrude: function(schema, sname, data, exclusive, connections, target_user, dramatic) {
		// TODO: Give this the same flexibility as checkTypes and write a cohesive wrapper that checks types and rules.
		var checks = [];
		var security = this;
		if ('rules' in schema && connections) {
			schema.rules.forEach( function(rulev, rulei, rulea) {
				// console.log("Rule.");
				// console.log(rulev);
				if ('type' in rulev) {
					if (rulev['type'] == 'unique' && 'parameters' in rulev && 'fields' in rulev.parameters && rulev.parameters.fields.length > 0) {
						// console.log("Starting uniqueness check.");
						var qq = {};
						rulev.parameters.fields.forEach( function(fval, find, farr) {
							if (fval in data) {
								qq[fval] = data[fval];
							} else {
								checks.push(Promise.reject(new Error("Fields that must be unique must exist.")));
							}
						});
						// Add fixed constraints to the duplicate search.
						if ('qualifier' in rulev.parameters) {
							var qqtname;
							for (qqtname in rulev.parameters.qualifier) {
								qq[qqtname] = rulev.parameters.qualifier[qqtname];
							}
						}
						if ('destroyed_at' in schema.fields) {
							qq['destroyed_at'] == null;
						}
						checks.push(security.app.service(sname).find({query: qq}).then( function(rv) {
							if ('data' in rv && rv.data.length > 0) return Promise.reject(new Error("This combination already exists."));
						}));
					}
				}
			});
		}
		if (dramatic) {
			return Promise.all(checks);
		}
		return Promise.all(checks).then(function (rv) { return 0; }, function (err) { return -1; });
	},
	hookCreationPreflight: function(target_class, hook) {
		if (('provider' in hook.params && hook.params['provider'] != "") || 'user' in hook.params) {
			// console.log("Checking types and access.");
			return hook.app.security.checkTypesCreate(hook.app.security.data_schema[target_class], hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1, null).then( function (iv) {
				// console.log("Checking access succeeded.");
				if (iv < 0) return Promise.reject(new Error("We cannot write this."));
				// console.log("Checking rules.");
				return hook.app.security.checkRulesCrude(hook.app.security.data_schema[target_class], target_class, hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1).then(function () {
					hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
					return Promise.resolve(hook);
				}, function (err) { return Promise.reject(err); });
			}, function (err) { return Promise.reject(err); } );
		}
	},
	hookCreationOverlayPreflight: function(target_class, hook) {
		// Check types.
		// Break into overlay and non-overlay.
		// Create the base record.
		if (('provider' in hook.params && hook.params['provider'] != "") || 'user' in hook.params ||
				('query' in hook.params && '$overlay' in hook.params.query && hook.params.query['$overlay']) ||
				('overlay' in hook.params && hook.params.overlay)) {
			// console.log("Checking types and access.");
			// Check types.
			return hook.app.security.checkTypesCreate(hook.app.security.data_schema[target_class], hook.data, 1, 1, (('user' in hook.params) ? hook.params.user[hook.app.security.id_name] : null), 1, null).then( function (iv) {
				// console.log("Checking access succeeded.");
				if (iv < 0) return Promise.reject(new Error("We cannot write this."));
				return hook.app.security.checkRulesCrude(hook.app.security.data_schema[target_class], target_class, hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1).then(function () {
					// Split out overlay data if applicable.
					if ('overlay_name' in hook.app.security.data_schema[target_class]) {
						var sd = hook.app.security.splitPatch.apply(hook.app.security, [hook.app.security.data_schema[target_class].fields, hook.data]);
						hook.data = sd.baseData;
						hook.overlayData = sd.overlayData;
					}
					hook.app.security.hookAddCreationDestructionInPlaceBefore(hook);
					return Promise.resolve(hook);
				}, function (err) { return Promise.reject(err); });
			}, function (err) { return Promise.reject(err); } );
		}
	},
	hookCreationOverlayPostflightFull: function(target_class, hook, hybridUpdate, patching) {
		// This works for updates also.
		if ('overlayData' in hook && "overlay_name" in hook.app.security.data_schema[target_class] && hook.app.security.data_schema[target_class].overlay_name) {
			// Populate dependent fields.
			if (hybridUpdate) {
				hook.overlayData.base_id = hook.id;
				hook.overlayData.created_at = hookTimestamp(hook);
				if ('user' in hook.params)
					hook.overlayData.created_by = hook.params.user[hook.app.security.id_name];
				hook.overlayData.destroyed_at = null;
			} else {
				hook.overlayData.base_id = hook.result[hook.app.security.id_name];
				hook.overlayData.created_at = hook.result.created_at;
				if ('user' in hook.params)
					hook.overlayData.created_by = hook.result.created_by;
				hook.overlayData.destroyed_at = null;
			}
			var removal = Promise.resolve(0);
			// Remove any existing overlay if this is an update.
			if ('id' in hook) removal = hook.app.service(hook.app.security.data_schema[target_class].overlay_name).find({query: {base_id: hook.id, destroyed_at: null}}).then(
				function (fres) {
					var rmops = [];
					if ('data' in fres) {
						fres.data.forEach( function (val, ind, arr) {
							rmops.push(hook.app.service(hook.app.security.data_schema[target_class].overlay_name).patch(val[hook.app.security.id_name], {destroyed_at: hook.result.created_at, destroyed_by: hook.result.created_by}, {}));
						} );
					}
					return Promise.all(rmops);
				}, function (err) { return Promise.reject(err); }
			);
			return removal.then(
				function (rres) {
					// Create the overlay.
					return hook.app.service(hook.app.security.data_schema[target_class].overlay_name).create(hook.overlayData);
				}, function (err) { return Promise.reject(err); }
			)
			.then(function (res) {
				// We get a fake result from the before hook in the case of an update.
				if ('resultFake' in hook && hook.resultFake && 'id' in hook) {
					// But we want to return the actual record to the requester.
					return hook.app.service(target_class).get(hook.id).then(
						function (data) {
							hook.result = data;
							return Promise.resolve(hook);
						},
						function (err) {
							return Promise.reject(err);
						}
					);
				}
				return Promise.resolve(hook);
			}, function (err) { return Promise.reject(err); });
		}
		return Promise.resolve(hook);
		// TODO: Write the operations to the pending mirror operation table.
	},
	hookCreationOverlayPostflight: function(target_class, hook) {
		return hook.app.security.hookCreationOverlayPostflightFull(target_class, hook, 0, 0);
	},
	hookUpdateOverlayPreflightFull: function(target_class, hook, baseUpdate, patching) {
		if (('provider' in hook.params && hook.params['provider'] != "") || 'user' in hook.params ||
				('query' in hook.params && '$overlay' in hook.params.query && hook.params.query['$overlay']) ||
				('overlay' in hook.params && hook.params.overlay)) {
			// Check access to the existing record.
			var old_cache = null;
			var pcheck = hook.app.security.userCanWrite((('user' in hook.params) ? hook.params.user[hook.app.security.id_name] : null), target_class, hook.id).then(
				function (can_access) {
					if (!can_access) return Promise.reject(new Error("No access to this record."));
					// Get the original record for reference if necessary.
					var original_check = Promise.resolve(null);
					if (('compare_old' in hook.app.security.data_schema[target_class] && hook.app.security.data_schema[target_class].compare_old) ||
							patching) {
						return hook.app.service(target_class).get(hook.id, {query: {'$overlay': 1}}).then(
							function (original) {
								old_cache = original;
								// Check types.
								if (patching) {
									// We independently check the validity of the patch and the validity of the total patched data.
									var echecks = [];
									echecks.push(hook.app.security.checkTypesPatchOverlay(hook.app.security.data_schema[target_class], hook.data, 1, 1, (('user' in hook.params) ? hook.params.user[hook.app.security.id_name] : null), 1, original));
									var virtual_future = hook.app.security.mergePatch(original, hook.data);
									// console.log("Original.");
									// console.log(original);
									// console.log("Patch.");
									// console.log(hook.data);
									// console.log("Merge.");
									// console.log(virtual_future);
									// Remove unwritable fields.
									var tfield;
									for (tfield in virtual_future) {
										// console.log(tfield);
										// console.log(virtual_future[tfield]);
										if (tfield in hook.app.security.data_schema[target_class].fields && 'is_user_writable' in hook.app.security.data_schema[target_class].fields[tfield] && hook.app.security.data_schema[target_class].fields[tfield]['is_user_writable'] == 0) {
											// console.log("Dropping " + tfield + ".");
											delete virtual_future[tfield];
										}
									}
									echecks.push(hook.app.security.checkTypesUpdateOverlay(hook.app.security.data_schema[target_class], virtual_future, 1, 1, (('user' in hook.params) ? hook.params.user[hook.app.security.id_name] : null), 1, null));
									return Promise.all(echecks);
								} else {
									// If this is an update, we need to fetch the previous record for reference.
									return hook.app.security.checkTypesUpdateOverlay(hook.app.security.data_schema[target_class], hook.data, 1, 1, hook.params.user[hook.app.security.id_name], 1, original);
								}
							}, function (err) { return Promise.reject(err); }
						);
					} else {
						return hook.app.security.checkTypesUpdateOverlay(hook.app.security.data_schema[target_class], hook.data, 1, 1, (('user' in hook.params) ? hook.params.user[hook.app.security.id_name] : null), 1, null);
					}
				}, function (err) { return Promise.reject(err); }
			);
			return pcheck.then( function (iv) {
				// Note that iv is meaningless. It might be even be an array.
				// console.log("Checking access succeeded.");
				// Split out overlay data if applicable.
				if ('overlay_name' in hook.app.security.data_schema[target_class]) {
					var sd = hook.app.security.splitPatch.apply(hook.app.security, [hook.app.security.data_schema[target_class].fields, hook.data]);
					hook.data = sd.baseData;
					hook.overlayData = sd.overlayData;
					if (patching) {
						// If patching, we need to attach the old data to the overlay, as those are not stacked.
						// Cache data from the previous operation.
						if (old_cache == null) {
							return Promise.reject("Cache miss in patching hook.");
						}
						var osd = hook.app.security.splitPatch.apply(hook.app.security, [hook.app.security.data_schema[target_class].fields, old_cache]);
						var tmp_overlay = hook.app.security.mergePatch(osd.overlayData, hook.overlayData);
						hook.overlayData = tmp_overlay;
					}
				}
				// hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
				if (baseUpdate == 1) {
					// We need to attach the old unwritable values to the new record.
					return hook.app.service(target_class).get(hook.id).then(function (original) {
						var ofd;
						for (ofd in original) {
							if (ofd in hook.app.security.data_schema[target_class].fields && 'is_user_writable' in hook.app.security.data_schema[target_class].fields[ofd] && hook.app.security.data_schema[target_class].fields[ofd].is_user_writable == 0) {
								hook.data[ofd] = original[ofd];
							}
						}
						return hook;
					}, function (err) { return Promise.reject(err); });
				}
				// We skip updating the base record, but we populate fields in the fake result for the benefit of the after hook.
				hook.result = {};
				var tm;
				for (tm in hook.data) { hook.result[tm] = hook.data[tm]; }
				hook.result[hook.app.security.id_name] = hook.id;
				hook.result.created_at = hookTimestamp(hook);
				if ('user' in hook.params)
					hook.result.created_by = hook.params.user[hook.app.security.id_name];
				return Promise.resolve(hook);
			}, function (err) { return Promise.reject(err); } );
		} else if ('provider' in hook.params && hook.params['provider'] != "") {
			return Promise.reject(new Error("Missing user."));
		} else {
			// console.log("Update overlay preflight almost finished.");
			// hook.app.security.hookAddCreationInformationInPlaceBefore(hook);
			return Promise.resolve(hook);
		}
	},
	hookUpdateOverlayPreflight: function(target_class, hook) {
		return hook.app.security.hookUpdateOverlayPreflightFull(target_class, hook, 0, 0);
	},
	hookPatchOverlayPreflight: function(target_class, hook) {
		return hook.app.security.hookUpdateOverlayPreflightFull(target_class, hook, 0, 1);
	},
	hookUpdateOverlayPostflight: function(target_class, hook) {
		return hook.app.security.hookCreationOverlayPostflight(target_class, hook);
	},
	hookStripOverlayFlags: function(hook) {
		if ('query' in hook.params && '$overlay' in hook.params.query) {
			if (hook.params.query['$overlay'] === 0 || hook.params.query['$overlay'] === 1) {
				hook.params.overlay = hook.params.query['$overlay'];
			}
			delete hook.params.query['$overlay'];
		}
		return hook;
	},
	hookChooseOverlay: function(hook) {
		if ('params' in hook) {
			if ('provider' in hook.params && hook.params.provider != null) {
				// Outside queries get the overlay by default.
				if ('query' in hook.params && '$overlay' in hook.params.query && hook.params.query['$overlay'] == 0) {
					return 0;
				} else if ('overlay' in hook.params && hook.params.overlay == 0) {
					return 0;
				}
				return 1;
			} else {
				if ('query' in hook.params && '$overlay' in hook.params.query && hook.params.query['$overlay'] == 1) {
					return 1;
				} else if ('overlay' in hook.params && hook.params.overlay == 1) {
					return 1;
				}
			}
		}
		return 0;
	},
	attachOverlay: function(target_class, rec) {
		var thisC = this;
		if ('overlay_name' in thisC.data_schema[target_class] &&
				thisC.data_schema[target_class].overlay_name) {
			var searchParams = {'base_id': rec[thisC.id_name], destroyed_at: null};
			// console.log("Query.", searchParams);
			var qresult1p = thisC.app.service(thisC.data_schema[target_class].overlay_name).find({query: searchParams});
			return qresult1p.then(function(overlays) {
				if (!('data' in overlays)) return Promise.reject(new Error("Missing data."));
				// We assume that there is only one active overlay at a time.
				if (overlays.data.length > 0) {
					// Merge the overlay data.
					return Promise.resolve((thisC.mergeSchemedPatch(thisC.data_schema[target_class].fields, rec, overlays.data[0])).data);
				}
				return Promise.resolve(rec);
			}, function (err) { return Promise.reject(err); });
		}
		console.log("Missing overlay name.");
		return Promise.resolve(rec);
	},
	hookGetOverlayPostflight: function(target_class, hook) {
		var thisC = hook.app.security;
		if ('result' in hook) {
			return thisC.attachOverlay(target_class, hook.result)
			.then( function (rv) {
				hook.result = rv;
				return Promise.resolve(hook);
			}, function (err) { return Promise.reject(err); } );
		}
		return Promise.reject(new Error("Missing returned record."));
	},
	hookFindOverlayPostflight: function(target_class, hook) {
		// This is modified from hookGetOverlayPostflight with parallel promise support.
		var thisC = hook.app.security;
		if ('overlay_name' in hook.app.security.data_schema[target_class] &&
				hook.app.security.data_schema[target_class].overlay_name.length > 0) {
			if ('result' in hook && 'data' in hook.result) {
				// Create space for the subfetching promises.
				var fetches = new Array(hook.result.data.length);
				fetches.fill(null);
				// Fetch and merge data.
				hook.result.data.forEach( function (rec, ind, arr) {
					fetches[ind] = thisC.attachOverlay(target_class, rec);
				} );
				return Promise.all(fetches).then( function (rv) {
					hook.result.data = rv;
					return Promise.resolve(hook);
				}, function (err) { return Promise.reject(err); } );
			}
			return Promise.reject(new Error("Missing returned record."));
		}
		// return Promise.reject(new Error("Missing overlay name."));
		return Promise.resolve(hook);
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
	hookAuthorizationCanPatch: function(hook) {
    if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		return Promise.reject(new Error("Patching authority records is not a thing."));
	},
	hookAuthorizationPatch: function (hook) {
		// TODO: Remove this after fixing legacy products.
		return hookAuthorizationCanPatch(hook);
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
				return service.patch(hook.id, { destroyed_at: hookTimestamp(hook) }, hook.params).then(function(data) {
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
	},
	hookAuthorizationCanRemove: function(hook) {
		if (!('provider' in hook.params && hook.params['provider'] != "") && !('user' in hook.params)) return Promise.resolve(hook);
		if (hook.id == null) return Promise.reject(new Error("Must have record id."));
		return hook.app.security.checkAuthorityRevocation(hook.id, hook.params.user[hook.app.security.id_name]).then( function (iv) {
			if (iv >= 0) {
				return hook.app.service('authorities').get(hook.id).then( function (data) {
					if ('target_class' in data && this.validateText('max:255|alpha_dash', data['target_class']) >= 0 &&
							data['target_class'] in this.data_schema &&
							this.data_schema[data['target_class']] instanceof Object) {
						// We copy some stuff from the Feathers documentation.
						return hook.app.service(data['target_class']).patch(hook.id, { destroyed_at: hookTimestamp(hook) }, hook.params).then(function(data) {
							// Set the result from `patch` as the method call result
							hook.result = data;
							// Always return the hook or `undefined`
							return hook;
						});
					}
				}, function (err) { return Promise.reject(err); });
			} else {
				hook.result = {};
				return Promise.resolve(hook);
			}
		});
	},
	checkTieredHold: function(hold_path, hold_level) {
		// Ironically, we set hold_level to 2 here on read operations since that is all that is applicable.
		// And set it to 1 on write operations since any operation matters.
		var security = this;
		if (frankenlib.check_path_in_dict(security, ["named_holds"])) {
			if (treeDigToTrue(security.named_holds, hold_path, hold_level, true))
				return true;
			return false;
		}
		return false;
	},
	setTieredHold: function(hold_path, hold_level) {
		// hold_level 0 means release, 1 means read, and 2 means write.
		// Read locking is not yet working.
		var security = this;
		if (frankenlib.check_path_in_dict(security, ["named_holds"])) {
			if (treeSetToValue(security.named_holds, hold_path, hold_level, (hold_level == 0), true))
				return true;
			return false;
		}
		return false;
	}
};

// This tree stuff seemed like a good idea, but it does not allow parallel read locks.

function treeDigToTrue(tree, path, level, deep) {
	// Follow the path in the tree. Return true if the target or any of its prospective parents is true.
	// If deep is set, also return true if any children of the target are true.
	if (tree instanceof Object) {
		if (path.length > 0 && typeof(path[0]) == "string" && path[0].length > 0 && path[0] in tree)
			return treeDigToTrue(tree[path[0]], path.slice(1), level, deep);
		else if (path.length == 0 && deep) {
			var tname;
			for (tname in tree)
				if (treeDigToTrue(tree[tname], path.slice(1), level, deep)) return true;
		}
	} else if (tree === true || tree >= level) return true;
	return false;
}

function treeSetToValue(tree, path, v, d, c) {
	// v is the target value.
	// d is the delete flag.
	// c is the clean flag.
	// When the clean flag is set, this deletes any empty parents of the target node (as after deletion of that node).
	console.log(tree);
	console.log(path);
	if (tree instanceof Object) {
		if (path.length > 0 && typeof(path[0]) == "string" && path[0].length > 0) {
			console.log("Going.");
			if (path.length == 1) {
				console.log("Found it.");
				if (d) { if (path[0] in tree) delete tree[path[0]]; return null; }
				tree[path[0]] = v;
				return v;
			} else {
				if (!(d || c) && !(path[0] in tree)) tree[path[0]] = {};
				var t = null;
				if (path[0] in tree) {
					t = treeSetToValue(tree[path[0]], path.slice(1), v, d, c);
					if (c && tree[path[0]] instanceof Object && Object.keys(tree[path[0]]).length == 0)
						delete tree[path[0]];
				}
				return t;
			}
		}
	}
	return null;
}

function treeClean(tree) {
	if (tree instanceof Object) {
		var tname;
		for (tname in tree)
			if (tree[tname] instanceof Object) {
				if (Object.keys(tree[tname]).length) treeClean(tree[tname]);
				if (!Object.keys(tree[tname]).length) delete tree[tname];
			}
	} else if (tree) return true;
	return true;
}

function hookDoWithReadLock(hook, tododo) {
	// tododo must always return a promise, even on an error, or the lock gets stuck.
	// if ('lock' in hook) throw new Error("This hook event already has a lock!");
	if ('lock' in hook && 'success' in hook.lock && hook.lock.success == 1) return Promise.resolve(tododo());
	if (!('provider' in hook.params && hook.params['provider'] != "")) {
		// If the access is internal, we assume that necessary locks are already acquired.
		return Promise.resolve(tododo());
	} else {
		if ('lock' in hook) console.log("OVERWRITING LOCK!");
		hook.lock = {};
		// We create a promise, to resolve when the lock is in place.
		hook.lock.promise = new Promise( function(resolve, reject) {
			hook.lock.resolve = resolve;
			hook.lock.reject = reject;
		});
		// We request a lock.
		hook.app.lock.readLock(function (release) {
			// console.log("Hook read lock acquired.");
			// Once we have the lock, we store the release function.
			hook.lock.release = release;
			// And we resolve the locking promise so that the query can continue.
			// But we also release the lock if the inner work fails.
			hook.lock.resolve(tododo().then(
				function (rv) { hook.lock.success = 1; return Promise.resolve(rv); },
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

function hookLockRead(hook) {
	return hookDoWithReadLock(hook, function () { return Promise.resolve(hook); });
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
		return tmpp.then(function(rv) { /* console.log("Protected work done."); */ local_lock.resolve(rv); /* console.log("Releasing."); */ release(); /* console.log("Returning."); */ return Promise.resolve(rv); }, function(err) { local_lock.reject(err); release(); return Promise.reject(err); });
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
		if ('lock' in hook) console.log("OVERWRITING LOCK!");
		hook.lock = {};
		// We create a promise, to resolve when the lock is in place.
		hook.lock.promise = new Promise( function(resolve, reject) {
			hook.lock.resolve = resolve;
			hook.lock.reject = reject;
		});
		// We request a lock.
		hook.app.lock.writeLock(function (release) {
			// console.log("Hook write lock acquired.");
			// Once we have the lock, we store the release function.
			hook.lock.release = release;
			// And we resolve the locking promise so that the query can continue.
			hook.lock.resolve(tododo().then(
				function (rv) { hook.lock.success = 1; return Promise.resolve(rv); },
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

function hookLockWrite(hook) {
	return hookDoWithWriteLock(hook, function () { return Promise.resolve(hook); });
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

function doPromiseWithLock(lock, tododo, lock_type, timeout) {
	var local_lock = {};
	// We create a promise, to resolve when the lock is in place.
	local_lock.promise = new Promise( function(resolve, reject) {
		local_lock.resolve = resolve;
		local_lock.reject = reject;
	});
	var lock_function = lock.readLock;
	if (lock_type == "write") lock_function = lock.writeLock;
	var task_valid = 1;
	var valid_lock = new ReadWriteLock();
	// If there is a time-out (which refers specifically to lock acquisition), start the clock.
	if (timeout != undefined && timeout != null) {
		setTimeout(function () {
			return valid_lock.writeLock(function (lrelease) {
				if (task_valid) {
					task_valid = 0;
					local_lock.reject(new Error("The operation timed out."));
				}
				return lrelease();
			});
		}, timeout);
	}
	// We request a lock.
	lock_function(function (release) {
		// This runs when the lock becomes available.
		// We resolve the locking promise to the query result, but we inject a release instruction.
		if (timeout == null || timeout == undefined) {
			return tododo().then(function(rv) { local_lock.resolve(rv); release(); return Promise.resolve(rv); }, function(err) { local_lock.reject(err); release(); return Promise.reject(err); });
		} else {
			return valid_lock.writeLock(function (lrelease) {
				if (task_valid) {
					task_valid = 0;
					lrelease();
					return tododo().then(function(rv) { local_lock.resolve(rv); release(); return Promise.resolve(rv); }, function(err) { local_lock.reject(err); release(); return Promise.reject(err); });
				} else {
					lrelease();
					return release();
				}
				return null;
			});
		}
	});
	// We return that promise.
	return local_lock.promise;
}

function hookErrUnlock(hook, err) {
	var nHook = hookUnlock(hook);
	return Promise.reject(err);
}

function hookUnlock(hook) {
	if (!('provider' in hook.params && hook.params['provider'] != "")) {
		// We are locking only for the top-level (outside) access, so we are unlocking only at the end of that.
		return hook;
	}
	if ('lock' in hook) {
		if ('release' in hook.lock && typeof(hook.lock.release) == 'function') {
			hook.lock.release();
			// console.log("Hook lock released.");
		}
		delete hook.lock;
	}
	return hook;
}

function hookComposite(thisC, hArray, errH, hook) {
	// This chains the functions in the array (passing the return of one to the next) and exits on an error.
	var rv = Promise.resolve(hook);
	var erv = null;
	hArray.forEach(function (cfunc, c_i, f_a) {
		if (!erv) {
			// The resolutions are asynchronous, so this flag seldom does anything.
			var tv = rv.then(function (frv) {
				return Promise.resolve(cfunc.apply(thisC, [frv]))
				.then(
					function (rv) { return Promise.resolve(rv); },
					function (err) {
						// console.log("Error in hook.");
						erv = err; return Promise.reject(err);
					}
				)
			}, function (err) {
				return Promise.reject(err); }
			);
			rv = tv;
		}
	});
	return rv.then(function (rrv) {
		if (erv) {
			if (errH) {
				// console.log("Calling error handler.");
				return errH(hook, erv);
			} else {
				// console.log("Rejecting directly to error.");
				return Promise.reject(erv);
			}
		}
		return rv;
	}, function (err) {
		if (errH) {
			// console.log("Calling error handler.");
			return errH(hook, err);
		} else {
			// console.log("Rejecting directly to error.");
			return Promise.reject(err);
		}
		return Promise.reject(err);
	});
}

function hookCompositor(hArray, errH) {
	return function(hook) {
		return hookComposite(this, hArray, errH, hook);
	};
}

function queryDropSpecial(tin) {
	if (typeof(tin) == "number" || typeof(tin) == "string" || tin == null || tin == undefined || tin instanceof Date) return tin;
	if (tin instanceof Array) {
		var outa = [];
		tin.forEach(function (itt) { outa.push(queryDropSpecial(itt)); });
		return outa;
	}
	if (tin instanceof Object) return null;
	var tree = tin;
	var out = {};
	var tname;
	for (tname in tree) {
		if (typeof(tname) == "string" && tname.length > 0 && tname.charAt(0) != "$")
			out[tname] = queryDropSpecial(tree[tname]);
	}
	return out;
}

function escapeRegExp(str) {
	// https://stackoverflow.com/questions/3446170/escape-string-for-use-in-javascript-regex
	return str.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
}

function convertQueryStringSubstring(tree) {
	var out = {};
	var tname;
	for (tname in tree) {
		if (typeof(tname) == "string" && tname.length > 0 && tname.charAt(0) != "$") {
			if (typeof(tree[tname]) == "string")
				out[tname] = {"$substring": tree[tname]};
			else if (tree[tname] instanceof Object)
				out[tname] = convertQueryStringSubstring(tree[tname]);
			else
				out[tname] = tree[tname];
		} else {
			out[tname] = tree[tname];
		}
	}
	return out;
}

function convertQueryBlankStringDrop(tree) {
	var out = {};
	var tname;
	for (tname in tree) {
		if (typeof(tname) == "string" && tname.length > 0 && tname.charAt(0) != "$") {
			if (tree[tname] !== "") {
				if (tree[tname] instanceof Object && tree[tname])
					out[tname] = convertQueryBlankStringDrop(tree[tname]);
				else
					out[tname] = tree[tname];
			}
		} else {
			out[tname] = tree[tname];
		}
	}
	return out;
}

function convertQuerySubstringRegExp(tree) {
	var out = {};
	var tname;
	for (tname in tree) {
		if (tname == "$substring") {
			if (typeof(tree[tname]) == "string" && tree[tname].length > 0)
				out["$regex"] = new RegExp(escapeRegExp(tree[tname]));
		} else {
			if (tree[tname] instanceof Object) {
				out[tname] = convertQuerySubstringRegExp(tree[tname]);
			} else {
				out[tname] = tree[tname];
			}
		}
	}
	return out;
}

function hookQueryStringSubstring(hook) {
	if ("query" in hook.params) hook.params.query = convertQueryStringSubstring(hook.params.query);
	return hook;
}

function hookQuerySubstringRegExp(hook) {
	if ("query" in hook.params) hook.params.query = convertQuerySubstringRegExp(hook.params.query);
	return hook;
}

function errorPasser(err) {
	return Promise.reject(err);
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
	rv['models'] = {};
	return rv;
}

exports = module.exports = {create: reprivilegerCreate, createTestApp: reprivilegerCreateTestApp, testTestApp: reprivilegerTestTestApp, hookDoWithReadLock: hookDoWithReadLock, hookDoWithWriteLock: hookDoWithWriteLock, doWithReadLock: doPromiseWithReadLock, doWithWriteLock: doPromiseWithWriteLock, doPromiseWithLock: doPromiseWithLock, hookLockRead: hookLockRead, hookLockWrite: hookLockWrite, hookUnlock: hookUnlock, errorPasser: errorPasser, hookCompositor: hookCompositor, hookErrUnlock: hookErrUnlock, escapeRegExp: escapeRegExp, convertQueryStringSubstring: convertQueryStringSubstring, convertQuerySubstringRegExp: convertQuerySubstringRegExp, convertQueryBlankStringDrop: convertQueryBlankStringDrop, hookQueryStringSubstring: hookQueryStringSubstring, hookQuerySubstringRegExp: hookQuerySubstringRegExp, queryDropSpecial: queryDropSpecial,
	tokenizeValidationRule: tokenizeValidationRule,
	validateText: validateText,
	checkTypesNested: checkTypesNested,
	checkTypes: checkTypes,
	coerceNumericToIntegerPatch: coerceNumericToIntegerPatch,
	splitPatch: splitPatch,
	mergePatch: mergePatch,
	mergeSchemedPatch: mergeSchemedPatch,
	mergeSchemedPatchInPlace: mergeSchemedPatchInPlace,
	populateHierarchy: populateHierarchy,
	splitSubmodelData: splitSubmodelData,
	flattenHierarchy: flattenHierarchy,
	mergeSubmodelData: mergeSubmodelData,
	splitSubmodelResultHook: convertSubmodelFlattenByMap,
	inlineSchema: inlineSchema,
	defaultValueDataFromSchema: defaultValueDataFromSchema,
	dictDiff: dictDiff,
	schemaGetQuantitativeNames: schemaGetQuantitativeNames,
	vectorTallyWithSchemaArrays: vectorTallyWithSchemaArrays,
	vectorAverageWithSchemaByName: vectorAverageWithSchemaByName,
	padZero: padZero,
	generateFormCrude: generateFormCrude,
	numberCheck: numberCheck,
	extractFormData: extractFormData,
	filterFormData: filterFormData,
	vectorDifferenceWithSchemaByName: vectorDifferenceWithSchemaByName,
	easyCompute: easyCompute,
	coerceValues: coerceValues,
	includeEasyComputedValues: includeEasyComputedValues,
	generateValidatorSchema: generateValidatorSchema
};

