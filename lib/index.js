var
  Embedded = require('nedb'),
  traverse = require('traverse'),
  _ = require('underscore'),
  ce = require('cloneextend'),
  async = require('async');

module.exports = EmbeddedProvider;

function EmbeddedProvider(options, helpers, callback) {
  if (!(this instanceof EmbeddedProvider)) return new EmbeddedProvider(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'Embedded';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;

  this.client = null;
  this.db = null;
  this.dbs = {};

  return this.init(options, function (err) {
    if (err)
      return callback(err);

    return callback(null, self);
  });
}

EmbeddedProvider.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;
  return callback();
  //return self.openConnection(options, callback);
};

EmbeddedProvider.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  var calls = [];
  Object.keys(self.dbs).forEach(function (key) {
    var db = self.dbs[key];
    var call = function (callback) {
      self.logger.info('Destroying connection to database [' + key + '].');
      self.closeConnection(db, callback);
    };
    calls.push(call);
  });
  async.series(calls, function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};

EmbeddedProvider.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;
  return callback(null);
};

EmbeddedProvider.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

EmbeddedProvider.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;
  return callback(null);
};

EmbeddedProvider.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };

  var self = this;

  if (!Array.isArray(documents))
    documents = [documents];

  var furnish = function (collection, documents, callback) {
    var bucket = {};

    var dimensions = [];
    var metrics = [];
    var dateDimensions = [];
    var _meta = self.common.extend(collection.meta);
    delete _meta.dimensions;
    delete _meta.metrics;
    delete _meta.description;
    delete _meta.storeKey;

    dateDimensions.push({key: 'timestamp'});
    documents.forEach(function (document, index) {
      var patched = false;
      dateDimensions.forEach(function (dateDimension) {
        var _dimension = self.common.extend({}, dateDimension);
        if (!document[_dimension.key]) {
          document[_dimension.key] = new Date().getTime();
          patched = true;
        }

        var _date = new Date(document[_dimension.key]);
        bucket.dow = new Date(_date).getDay();
        bucket.hod = new Date(_date).getHours();
        _date.setMilliseconds(0);
        bucket.second = new Date(_date);
        _date.setSeconds(0);
        bucket.minute = new Date(_date);
        _date.setMinutes(0);
        bucket.hour = new Date(_date);
        _date.setUTCHours(0, 0, 0, 0);
        bucket.ddate = new Date(_date);
        _date.setDate(1);
        bucket.month = new Date(_date);
        _date.setMonth(0);
        bucket.year = new Date(_date);

        document[dateDimension.key + '_timebucket'] = ce.clone(bucket);
        document[dateDimension.key] = new Date(document[_dimension.key]);//bucket.second;
      });

      document.ourTimestamp = new Date();
      var documentKey = {};
      dimensions.forEach(function (dimension) {
        //var d = collection.dimensions[key];
        if (['timestamp', '_key', 'ourTimestamp'].indexOf(dimension.key) === -1)
          documentKey[dimension.key] = document[dimension.key];
        else if (dimension.key === 'timestamp')
          documentKey[dimension.key] = document[dimension.key].getTime();
      });

      //this will ensure that if we assign the timestamp, there's no collision
      if (patched && documents.length > 1) {
        documentKey.index = index;
      }

      if (collection.unique || true)
        document._key = self.common.hash(JSON.stringify(documentKey).toString());
      else
        document._key = self.common.uuid();

      documents[index] = document;
    });

    return setImmediate(function () {
      return callback(null, collection, collection.meta);
    });
  };

  return furnish(collection, documents, function (err) {
    if (err)
      return callback(err);

    self.openConnection(cleanup(collection.storeKey), {}, function (err, collection) {
      if (err)
        return callback(err);

      if (!collection)
        return callback(new Error('Failed to insert document(s) into collection [' + collection + ']@[' + self.db.options.url + ']'));

      return collection.insert(documents, function (err, result) {
        if (err)
          require('util').inspect(err);
        return callback(err);

        documents.forEach(function (doc) {
          doc.saved = true;
        });
        return callback(null, result);
      });
    });
  });
};

EmbeddedProvider.prototype.buildQueryPlan = function (query, callback) {
  var self = this;
  var plan = {
    uid: self.common.uuid(),
    cost: 0,
    colQueries: {},
    query: query
  };
  var $match = {};
  var $project = {};
  var $group = {};
  var $sort = {};
  var $limit;

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    $match.timestamp = {$gte: query.timeframe.start, $lt: query.timeframe.end};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = {$limit: query.timeframe.last_n_items};
  }

  if (query.limit)
    $limit = {$limit: parseInt(query.limit)};

  if (query.filter) {
    query.filter.forEach(function (f) {
      if (f[1] == 'eq')
        $match[f[0]] = f[2];
      else {
        $match[f[0]] = {};
        $match[f[0]]['$' + f[1]] = f[2];
      }
    });
  }

  $group._id = {};
  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        $group._id[dimension.key] = '$' + dimension.key + '_' + query.interval;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $group._id[dimension.key] = '$' + (dimension.attribute || dimension.key);
        break;
      case 'geo':
        break;
      default:
        return setImmediate(function () {
          return callback(new Error('Dimension [' + dimension.key + '] has unknown type of [' + dimension.datatype + ']'));
        });
    }
  });

  if (query.metrics.length === 0) {
    try {
      query.metrics.push({
        key: 'fake',
        dependsOn: 'fake',
        collection: query.collection.key || query.dimensions ? query.dimensions[0].collection : null
      });
    }
    catch (ex) {
      query.metrics = [];
    }
  }

  query.sort = query.sort || query.orderby;
  if (query.sort && Array.isArray(query.sort)) {
    query.sort.forEach(function (s) {
      $sort[s[0]] = s[1].toUpperCase() === 'DESC' ? -1 : 1;
    });
  }
  else
    $sort['timestamp'] = -1;

  query.metrics.forEach(function (metric) {
    var colQuery = {
      collection: metric.collection ? metric.collection.key : null,
      query: []
    };

    if (!metric.formula && metric.collection) {
      if (metric.aggregation)
        metric.aggregation = metric.aggregation.toLowerCase();
      if (metric.aggregation == 'ucount')
        colQuery.type = 'ucount';
      else
        colQuery.type = 'plain';

      var _$match = self.common.extend({}, $match);

      var _$unwind;// = '$' + metric.dependsOn || metric._key;
      if (metric.dependsOn.indexOf('.') > 0 && self.common.checkNestedArray(metric.collection, metric.dependsOn))
        _$unwind = '$' + metric.dependsOn.substring(0, metric.dependsOn.indexOf('.')) || metric._key;
      var _$project = self.common.extend({}, $project);
      var _$group = self.common.extend({}, $group);
      var _$sort = self.common.extend({}, $sort);

      if (metric.filter) {
        metric.filter.forEach(function (f) {
          if (f[1] == 'eq')
            _$match[f[0]] = f[2];
          else {
            _$match[f[0]] = {};
            _$match[f[0]]['$' + f[1]] = f[2];
          }
        });
      }
      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match) + JSON.stringify(_$unwind));

      //if (colQuery.type == 'plain') {
      if (plan.colQueries[colQuery.key]) {
        if (_$unwind)
          _$group = self.common.extend({}, plan.colQueries[colQuery.key].query[2].$group);
        else
          _$group = self.common.extend({}, plan.colQueries[colQuery.key].query[1].$group);
      }

      if (metric.key !== 'fake') {
        _$group[metric.key] = {};
        if (metric.aggregation == 'count')
          _$group[metric.key].$sum = 1;
        /*else if (metric.aggregation == 'avg') {
         _$group[metric.key]['$' + (typeof metric.aggregation === 'undefined' ? 'sum' : metric.aggregation)] = '$' + metric.attribute;
         _$group[metric.key + '_count'] = {};
         _$group[metric.key + '_total'] = {};
         _$group[metric.key + '_count'].$sum = 1;
         _$group[metric.key + '_total'].$sum = '$' + metric.attribute;
         }*/
        else
          _$group[metric.key]['$' + (typeof metric.aggregation === 'undefined' ? 'sum' : metric.aggregation)] = '$' + metric.attribute;
      }
      if (_$unwind) {
        colQuery.query = [
          {$match: _$match},
          {$unwind: _$unwind},
          // {$project: _$project},
          {$group: _$group},
          {$sort: _$sort}
        ];
      }
      else {
        colQuery.query = [
          {$match: _$match},
          // {$project: _$project},
          {$group: _$group},
          {$sort: _$sort}
        ];
      }

      if ($limit) {
        colQuery.query.push($limit);
      }

      plan.colQueries[colQuery.key] = colQuery;
    }
  });

  //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  return setImmediate(function () {
    return callback(null, plan);
  });
};

EmbeddedProvider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };
  var self = this;

  return self.buildQueryPlan(query, function (err, queryplan) {
    var result = {
      dimensions: [],
      metrics: [],
      documents: [],
      queryplan: queryplan
    };

    var arrCols = _.toArray(queryplan.colQueries);
    if (arrCols.length === 0) {
      var output = {
        queryplan: queryplan
      };
      output.dimensions = queryplan.dimensions;
      output.metrics = queryplan.metrics;
      output.documents = [];
      return setImmediate(function () {
        return callback(null, output);
      });
    }

    return async.map(arrCols, function iterator(_query, next) {
      var collectionName = context.user.workspace + '_' + _query.collection;
      self.openConnection(cleanup(collectionName), {}, function (err, collection) {
        if (err)
          return next(err);
        //console.log(require('util').inspect(_query.query, {depth: null, colors: true}));
        var group = {
          key: function () {
            if (Object.keys(_query.query[1].$group._id).length === 0)
              return {__dummy: 1};

            var result = {};
            Object.keys(_query.query[1].$group._id).forEach(function (key) {
              var elem = _query.query[1].$group._id[key];
              if (key === 'timestamp')
                result.timestamp = 1;
              else
                result[elem.replace('$', '').replace('_', '.')] = 1;
            });
            return result;
          }(),
          reduce: function (curr, result) {
            Object.keys(_query.query[1].$group).forEach(function (key) {
              if (key !== '_id') {
                var elem = _query.query[1].$group[key];
                var operator = Object.keys(elem)[0];
                var attribute = elem[operator].replace('$', '');
                switch (operator) {
                  case '$sum':
                    result[key] = result[key] || 0;
                    result[key] += joola.common.flatGetSet(curr, attribute);
                    break;
                  case '$avg':
                    result[key] = result[key] || 0;
                    result['__count_' + key] = result['__count_' + key] || 0;
                    result[key] += joola.common.flatGetSet(curr, attribute);
                    result['__count_' + key]++;
                    break;
                  case '$min':
                    result[key] = result[key] || joola.common.flatGetSet(curr, attribute);
                    if (result[key] > joola.common.flatGetSet(curr, attribute))
                      result[key] = joola.common.flatGetSet(curr, attribute);
                    break;
                  case '$max':
                    result[key] = result[key] || joola.common.flatGetSet(curr, attribute);
                    if (result[key] < joola.common.flatGetSet(curr, attribute))
                      result[key] = joola.common.flatGetSet(curr, attribute);
                    break;
                  case '$ucount':
                    result['__bag_' + key] = result['__bag_' + key] || [];
                    if (result['__bag_' + key].indexOf(joola.common.flatGetSet(curr, attribute)) === -1)
                      result['__bag_' + key].push(joola.common.flatGetSet(curr, attribute))
                    break;
                  default:
                    break;
                }
              }
            });
          },
          initial: function () {
            var result = {};
            Object.keys(_query.query[1].$group).forEach(function (key) {
              if (key !== '_id') {
                var elem = _query.query[1].$group[key];
                //result[key] = 0;
                //result['__count_' + key] = 0;
              }
            });
            return result;
          }(),
          finalize: function (result) {
            result._id = {};

            Object.keys(_query.query[1].$group).forEach(function (key) {
              if (key !== '_id') {
                var elem = _query.query[1].$group[key];
                var operator = Object.keys(elem)[0];
                var attribute = elem[operator].replace('$', '');
                switch (operator) {
                  case '$avg':
                    result[key] = result[key] / result['__count_' + key];
                    break;
                  case '$ucount':
                    result[key] = result['__bag_' + key].length;
                    break;
                  default:
                    break;
                }
              }
            });
          }
        };
        var match = _query.query[0].$match;
        Object.keys(match).forEach(function (topKey) {
          var topElem = match[topKey];
          if (typeof topElem === 'object') {
            Object.keys(topElem).forEach(function (key) {
              var elem = topElem[key];
              if (key === '$regex')
                match[topKey][key]=new RegExp(elem, "i");
            });
          }
        });
        var sort = _query.query[2].$sort;
        //console.log(match, sort, group);
        collection.find(match).sort(sort).group(group).exec(function (err, results) {
          if (err)
            return next(err);
          result.dimensions = queryplan.dimensions;
          result.metrics = queryplan.metrics;

          //prevent circular references on output.
          result.metrics.forEach(function (m, i) {
            if (!m.formula) {
              if (m.collection)
                m.collection = m.collection.key;
              result.metrics[i] = m;
            }
          });
          results.forEach(function (r) {
            Object.keys(r).forEach(function (key) {
              if (key.indexOf('__') === 0)
                delete r[key];
            });
            r._id = {};
            //r.timestamp = new Date(r.timestamp);
            if (Object.keys(_query.query[1].$group._id).length > 0) {
              Object.keys(_query.query[1].$group._id).forEach(function (key) {
                var elem = _query.query[1].$group._id[key];
                r._id[key] = r[key];
                delete r[key];
              });
            }
          });
          //console.log('done', results);
          result.documents = results;
          return next(null, self.common.extend({}, result));
        });
      });
    }, function (err, results) {

      if (err)
        return setImmediate(function () {
          return callback(err);
        });

      var output = {
        queryplan: queryplan
      };
      var keys = [];
      var final = [];
      var stop_due_to_error = false;
      if (results && results.length > 0) {
        output.dimensions = results[0].dimensions;
        output.metrics = results[0].metrics;

        results.forEach(function (_result) {
          if (stop_due_to_error)
            return;
          _result.documents.forEach(function (document) {
            var key = self.common.hash(JSON.stringify(document._id));
            var row;

            if (keys.indexOf(key) == -1) {
              row = {};
              Object.keys(document._id).forEach(function (key) {
                row[key] = document._id[key];
              });
              row.key = key;
              keys.push(key);
              final.push(row);
            }
            else {
              row = _.find(final, function (f) {
                return f.key == key;
              });
            }

            Object.keys(document).forEach(function (attribute) {
              if (stop_due_to_error)
                return;
              if (attribute != '_id') {
                var d = _.find(output.dimensions, function (item) {
                  return item.key === attribute;
                });
                var m = _.find(output.metrics, function (item) {
                  return item.key === attribute;
                });

                var value = self.common.flatGetSet(document, attribute);
                if (d && typeof value === 'undefined') {
                  //row[d._key] = '(not set)';
                  self.common.flatGetSet(row, d._key, '(not set)');
                }
                else if (d)
                  self.common.flatGetSet(row, d._key, value);
                else if (m && !value)
                  self.common.flatGetSet(row, m._key, null);
                else if (m)
                  self.common.flatGetSet(row, m._key, value);
                else {
                  //console.log(row, m, d);
                  stop_due_to_error = true;
                  return callback(new Error('Mismatch between dimensions/metrics lookup and actual returned set.'))
                }
              }
            });
            output.metrics.forEach(function (m) {
              if (!row[m.key])
                row[m.key] = null;
            });
            output.dimensions.forEach(function (d) {
              if (typeof row[d.key] === 'undefined') {
                row[d.key] = '(not set)';
              }
            });
            final[keys.indexOf(key)] = row;
          });
        });
        output.documents = final;
        return setImmediate(function () {
          return callback(null, output);
        });
      }
      else {
        output.dimensions = queryplan.dimensions;
        output.metrics = queryplan.metrics;
        output.documents = [];
        return setImmediate(function () {
          return callback(null, output);
        });
      }
    });
  });
};

EmbeddedProvider.prototype.openConnection = function (name, options, callback) {
  callback = callback || function () {
  };

  var self = this;

  //check if we have a cached connection
  if (self.dbs[name])
    return self.checkConnection(self.dbs[name], callback);


  this.logger.trace('Open Embedded connection @ ' + name);
  self.dbs[name] = new Embedded({filename: './data/' + name + '.db', autoload: true});
  return callback(null, self.dbs[name]);
};

EmbeddedProvider.prototype.closeConnection = function (db, callback) {
  callback = callback || function () {
  };

  var self = this;

  return db.close(callback);
};

EmbeddedProvider.prototype.checkConnection = function (db, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, db);
};

EmbeddedProvider.prototype.stats = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, {});
};

EmbeddedProvider.prototype.drop = function (collectionName, callback) {
  callback = callback || function () {
  };

  var self = this;
  return callback(null, {});
};

EmbeddedProvider.prototype.purge = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null, {});
};

var cleanup = function (string) {
  return string.replace(/[^\w\s]/gi, '');
};