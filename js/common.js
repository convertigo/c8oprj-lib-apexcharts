var CouchKey = use("com.twinsoft.convertigo.engine.enums.CouchKey");
var CouchPostDocumentPolicy = use("com.twinsoft.convertigo.engine.enums.CouchPostDocumentPolicy");
var fsclient = com.twinsoft.convertigo.engine.Engine.theApp.couchDbManager.getFullSyncClient();

var debug = function() {
	var i;
	if (true) {
		var msg ="";
		for (i in arguments) {
			var arg = arguments[i];
			if (typeof arg == "string") {
				msg += arg;
			} else {
				try {
					msg += JSON.stringify(arg);
				} catch (e) {
					msg += "" + arg;
				}
			}
		}
		log.warn(msg);
	}
}

var datamodels = {};
var getDatamodel = function (datamodelName) {
	if (datamodelName in datamodels) {
		return datamodels[datamodelName]
	}
	var datamodel = toJSON(fsclient.getDocument(database, 'datamodel_' + datamodelName));
	datamodel._ = {
		name: {},
		key: {}
	};
	for (i in datamodel.fields) {
		j = datamodel.fields[i];
		datamodel._.name[j.name] = j;
		if ("source" in j && "key" in j.source) {
			datamodel._.key[j.source.key] = j;
		}
	}
	return datamodels[datamodelName] = datamodel;
}

var calculateIndex = function(obj, fields) {
	var k, i;
	if (!fields) {
		fields = [];
	}
	if (typeof obj != "object") {
		return fields;
	}
	for (k in obj) {
		if (typeof k != "string") {
			return fields;
		}
		if (!k.startsWith("$")) {
			var push = true;
			for (i = 0; i < fields.length; i++) {
				if (fields[i].startsWith(k)) {
					push = false;
					break;
				}
				if (k.startsWith(fields[i])) {
					fields.splice(i ,1);
					i--;
				}
			}
			if (push) {
				fields.push(k);
			}
			calculateIndex(obj[k], fields);
		}
	}
	return fields;
}

var toJSON = function(json) {
	return JSON.parse(json.toString());
}

var toJettison = function(json) {
	var txt = (typeof json == "string") ? json : JSON.stringify(json);
	return txt.startsWith("{") ? new org.codehaus.jettison.json.JSONObject(txt) : new org.codehaus.jettison.json.JSONArray(txt);
}

var fsQuery = function(database, json) {
	var fields = calculateIndex(json.where);
	var name = "index_" + JSON.stringify(json.partial).replace(new RegExp("[^\\w,]", "g"), "") + "_"
				+ JSON.stringify(fields).replace(new RegExp("[^\\w,]", "g"), "");
	json.use_index = [name, name];
	json.execution_stats = true;
	var body = {
		index: {
			fields: fields,
			partial_filter_selector: json.partial
		},
		ddoc: name,
		name: name,
		type: "json"
	};
	var request = new org.apache.http.client.methods.HttpPost(fsclient.getDatabaseUrl(database) + "/_index");
	fsclient.setJsonEntity(request, toJettison(body));
	var out = fsclient.execute(request);
	debug("fsQuery postIndex: ", "" + out);
	//fsclient.postIndex(database, name, name, toJettison(fields));
	json.selector = Object.assign({}, json.partial, json.where);
	delete json.where;
	delete json.partial;
	debug("fsQuery q: ", json);
	var res = toJSON(fsclient.postFind(database, toJettison(json)));
	if ("warning" in res) {
		log.warning("fsQuery index warning: " + res.warning);
	}
	try {
		if (res.execution_stats.execution_time_ms > 500) {
			log.warning("fsQuery long response > 500ms: " + json);
		}
	} catch (e) {
		
	}
	debug("fsQuery r: ", res);
	return res;
}

var multiIndex = function(obj, is) {  // obj,['1','2','3'] -> ((obj['1'])['2'])['3']
    return is.length ? multiIndex(obj[is[0]], is.slice(1)) : obj;
}

var pathIndex = function(obj, is) {   // obj,'1.2.3' -> multiIndex(obj,['1','2','3'])
    return multiIndex(obj, is.split('.'));
}

var getOrCreate = function(obj, key, create) {
	if (typeof create == "undefined") {
		create = {};
	}
	if (!obj[key]) {
		obj[key] = create;
	}
	return obj[key];
}

var uniq = function(ar) {
	return Object.keys(ar.reduce((acc,curr)=> (acc[curr]='',acc),{}));
}

var safeEval = function(expression, value) {
	var res = eval(expression);
	debug("safeEval: ", expression, " value: ", value, " res: ", res);
	return res;
}

var putIfFirst = function(obj, key, value) {
	if (!(key in obj)) {
		obj[key] = value;
	}
}

var countOk = function (res) {
	var i, cpt = 0;
	try {
		res = res.optJSONArray("items");
		for (i = 0; i < res.length(); i++) {
			if (res.optJSONObject(i).optBoolean("ok")) {
				cpt++;
			}
		}
	} catch (e) {}
	return cpt;
}

var doIndex = function(datamodel, index) {
	if (index.length == 1) {
		return index[0];
	}
	var indexName = index.join(' ~> ');
	var indexes = getOrCreate(datamodel, 'indexes', []);
	var i, j, found = false;
	for (i = 0; !found && i < indexes.length; i++) {
		var idx = indexes[i];
		if (found = (idx.length == index.length)) {
			for (j = 0; found && j < idx.length; j++) {
				if (!(index[j] in datamodel._.name)) {
					throw 'field "' + index[j] + '" "not found';
				}
				found = idx[j] == index[j];
			}
		}
	}
	if (!found) {
		debug('push indexName: "', indexName, '" index: ', index);
		indexes.push(index);
		var json = toJettison(datamodel);
		json.remove('_');
		fsclient.postDocument(database, json, null, CouchPostDocumentPolicy.override, null, true);
	}
	return indexName;
}

var doPreQuery = function(datamodel, kv) {
	var link, res, v;
	debug('doPreQuery datamodel: ', datamodel.name, ' kv.key: ', kv.key, ' kv.value: ', kv.value);
	if (kv.key in datamodel._.name) {
		var field = datamodel._.name[kv.key];
		if ('source' in field && 'link' in field.source) {
			link = datamodel.links[field.source.link];
			v = {};
			v[field.source.field] = kv.value;
			res = doQuery(link.datamodel, [link.remote], v, 1,0, false);
			debug("res: ", res);
			kv.key = link.local;
			kv.value = res.docs[0][link.remote];
			debug("kv: ", kv);
		}
	} else {
		k = kv.key.split('->');
		if (k.length > 1) {
			kv.key = k.shift();
			link = datamodel.links[kv.key];
			v = {};
			v[k.join('->')] = kv.value;
			res = doQuery(link.datamodel, [link.remote], v, 1, 0, false);
			debug("res: ", res);
			kv.key = link.local;
			kv.value = res.docs[0][link.remote];
			debug("kv: ", kv);
		}
	}
	return kv;
}

var doQuery = function(datamodelName, fields, where, limit, skip, descending, doCount) {
	var i, j, k, v, kv, key, keys, doc, value, view, indexName, link;
	var datamodel = getDatamodel(datamodelName);
	
	var query = new java.util.HashMap();
	keys = null;
	debug('where: ', where);
	if (where == null) {
		query.put('startkey', '["_id"]');
		query.put('endkey', '["_id", {}]');
	} else {
		if (!Array.isArray(where)) {
			where = [where];
		}
		var index = [];
		var values = [];
		for (i = 0; i < where.length - 1; i++) {
			kv = {};
			kv.key = Object.keys(where[i])[0];
			kv.value = where[i][kv.key];
			doPreQuery(datamodel, kv);
			index.push(kv.key);
			values.push(kv.value);
		}
		kv = {};
		var leaf = where[where.length - 1];
		if (typeof leaf == 'string') {
			if (leaf in datamodel._.name) {
				var field = datamodel._.name[leaf];
				if ('source' in field && 'link' in field.source) {
					link = datamodel.links[field.source.link];
					leaf = link.local;
				}
			}
			index.push(leaf);
			indexName = doIndex(datamodel, index);
			values.unshift(indexName);
			query.put('startkey', JSON.stringify(values));
			values.push({});
			query.put('endkey', JSON.stringify(values));
		} else if (typeof leaf == 'object') {
			kv.key = Object.keys(leaf)[0];
			kv.value = leaf[kv.key];
			if (typeof kv.value == 'string') {
				doPreQuery(datamodel, kv);
				index.push(kv.key);
				indexName = doIndex(datamodel, index);
				values.unshift(indexName);
				values.push(kv.value);
				query.put('key', JSON.stringify(values));
			} else if (typeof kv.value == 'object') {
				index.push(kv.key);
				indexName = doIndex(datamodel, index);
				values.unshift(indexName);
				if ('in' in kv.value) {
					j = kv.value.in;
					if (typeof j == 'string') {
						j = [j];
					}
					keys = [];
					for (i in j) {
						k = values.slice();
						k.push(j[i]);
						keys.push(k);
					}
				} else if ('[=' in kv.value) {
					values.push(kv.value['[=']);
					query.put('startkey', JSON.stringify(values));
					values[values.length - 1] += '\uffff';
					query.put('endkey', JSON.stringify(values));
				} else {
					if ('>=' in kv.value) {
						values.push(kv.value['>='])
					}
					query.put('startkey', JSON.stringify(values));
					values[values.length - 1] = '<=' in kv.value ? kv.value['<='] : {};  
					query.put('endkey', JSON.stringify(values));
				}
			}
		}
		debug('index: ', index);
		debug('values: ', values);
	}
	
	var ddoc = {
		_id: '_design/datamodel_' + datamodelName,
		views: {
			datamodel: {}
		}
	};
	var map = 'function (doc) {\n';
	map += 'try {if (!(' + datamodel.filter + ')) return;} catch (e) { return; }\n';
	map += 'var value = {};\n';
	var slinks = '';
	
	var subQueries = {};
	
	for (i in datamodel.fields) {
		var field = datamodel.fields[i];
		var needed = !fields.length || fields.indexOf(field.name) != -1; 
		if ('key' in field.source) {
			key = field.source.key;
			if ('adapter' in field.source) {
				map += 'value["' + field.name + '"] = function (value) {\n';
				map += field.source.adapter + '\n}(doc.' + key + ');\n';
			} else {
				map += 'value["' + field.name + '"] = doc.' + field.source.key + ';\n';
			}
		} else if ('keys' in field.source && 'adapter' in field.source) {
			k = field.source.keys;
			map += 'value["' + field.name + '"] = function (v1, v2, v3, v4, v5, v6, v7, v8) {\n';
			map += field.source.adapter + '\n}(';
			for (j in k) {
				if (j > 0) {
					map += ', ';
				}
				map += 'doc.' + k[j];
			}
			map += ');\n';
		} else if ((!fields.length || fields.indexOf(field.name) != -1) && 'link' in field.source && 'field' in field.source) {
			var subQueryLink = getOrCreate(subQueries, field.source.link);
			subQueryLink[field.name] = field.source.field;
		}
	}
	
	map += slinks + '\n';
	map += 'emit(["_id", (doc._id)], value);\n\n';
	
	map += '// fields\n';
	for (i in datamodel.fields) {
		var field = datamodel.fields[i];
		if ('source' in field && 'key' in field.source) {
			map += 'emit(["' + field.name + '", value["' + field.name + '"]], null);\n';
		}
	}
	map += '// indexes\n';
	if ('indexes' in datamodel) {
		for (i in datamodel.indexes) {
			var index = datamodel.indexes[i];
			map += 'emit(["' + index.join(' ~> ') + '"';
			for (j in index) {
				map += ', value["' + index[j] + '"]';
			}
			map += '], null);\n';
		}
	}
	
	map += '}';
	
	debug('map: ', map);
	ddoc.views.datamodel.map = map;
	ddoc.views.datamodel.reduce = '_count';
	var result = toJSON(fsclient.getDocument(database, ddoc._id, null));
	var exmap = null;
	try {
		exmap = result.views.datamodel.map;
	} catch (e) {}
	if (exmap != map) {
		result = '' + fsclient.postDocument(database, toJettison(ddoc), null, CouchPostDocumentPolicy.override, null, false);
		debug('postDesign: ', result);
	}
	

	if (limit > 0) {
		query.put('limit', '' + limit);
		query.put('skip', '' + skip);
	}
	
	if (doCount) {
		query.put('reduce', 'true');
		query.put('group', 'true');
	} else {
		query.put('reduce', 'false');
	}
	
	if (descending) {
		query.put('descending', 'true');
		if (query.containsKey('startkey')) {
			k = query.get('startkey');
			query.put('startkey', query.get('endkey'));
			query.put('endkey', k);
		}
	}
	debug('query: ' + query);
	if (keys != null) {
		debug('keys: ', keys);
		keys = toJettison(keys);
	    view = toJSON(fsclient.postView(database, 'datamodel_' + datamodelName, 'datamodel', query, keys));	
	} else {
	    view = toJSON(fsclient.getView(database, 'datamodel_' + datamodelName, 'datamodel', query));
	}
	
	debug('view: ', view);
	
	if (doCount) {
		result = {docs: []};
		for (i in view.rows) {
			doc = view.rows[i];
			debug('doc: ', doc);
			debug('where: ', where);
			v = {};
			k = where[where.length - 1];
			if (typeof k != 'string') {
				k = Object.keys(k)[0];
			}
			v.field = k;
			v.value = doc.key[doc.key.length - 1];
			v.count = doc.value;
			result.docs.push(v);
		}
		return result;
	}
	
	if (view.rows.length == 0) {
		where = null;
		fields = [];
		subQueries = {};
	}
	
	if (where != null) {
		keys = [];
		for (i in view.rows) {
			keys.push(['_id', view.rows[i].id]);
		}
		
		query = new java.util.HashMap();
		query.put('reduce', 'false');
		
		keys = toJettison(keys);
		view = toJSON(fsclient.postView(database, 'datamodel_' + datamodelName, 'datamodel', query, keys));
		debug('where result: ', view);
	}
	
	result = {docs: []};
	
	if (fields.length) {
		for (j in fields) {
			k = fields[j].split('->');
			if (k.length > 1) {
				key = k.shift();
				v = k.join('->');
				var name = key + '->' + v; 
				if (datamodel.links && key in datamodel.links) {
					var subQueryLink = getOrCreate(subQueries, key);
					subQueryLink[name] = v;
				}
			}
		}
	}
	
	debug('subQueries: ', subQueries);
	while (Object.keys(subQueries).length) {
		var linkName = Object.keys(subQueries)[0];
		var subQueryLink = subQueries[linkName];
		delete subQueries[linkName];
		link = datamodel.links[linkName];
		debug('linkName: ', linkName, ' subQueryLink: ', subQueryLink, ' link: ', link);
		var subFields = {};
		subFields[link.remote] = true;
		keys = Object.keys(subQueryLink);
		for (i = 0; i < keys.length; i++) {
			var localField = keys[i];
			var remoteField = subQueryLink[localField];
			debug('localField: ', localField, ' remoteField: ', remoteField);
			subFields[remoteField] = true;
		}
		
		debug('Object.keys(subFields): ', Object.keys(subFields));
		var whereIn = {};
		var adapted = {};
		for (i in view.rows) {
			doc = view.rows[i].value;
			debug('doc: ', doc);
			whereIn[doc[link.local]] = true;
		}
		var subWhere = {};
		subWhere[link.remote] = {"in": Object.keys(whereIn)};
		debug('subWhere: ', subWhere);
		
		var subResult = doQuery(link.datamodel, Object.keys(subFields), subWhere, -1, 0, false);
		var dictionary = {};
		for (i in subResult.docs) {
			var subDoc = subResult.docs[i];
			dictionary[subDoc[link.remote]] = subDoc;
		}
		debug('dictionary: ', dictionary);
		for (i in view.rows) {
			doc = view.rows[i].value;
			for (j = 0; j < keys.length; j++) {
				var localField = keys[j];
				var remoteField = subQueryLink[localField];
				doc[localField] = dictionary[doc[link.local]][remoteField];
			}
		}
	}
	
	debug('fields: ', fields);
	for (i in view.rows) {
		key = view.rows[i].id;
		value = view.rows[i].value;
		if (fields.length) {
			v = {};
			for (j in fields) {
				k = fields[j];
				v[k] = value[k];
			}
			v._id = key;
			result.docs.push(v);
		} else {
			value._id = key;
			result.docs.push(value);
		}
	}
	
	debug('result: ', result);
	return result;
}