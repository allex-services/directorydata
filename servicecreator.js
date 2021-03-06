var arrayoperationscreator = require('allex_arrayoperationslowlevellib');
function createDirectoryDataService(execlib, ParentService) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    dataSuite = execlib.dataSuite,
    arrayOperations = arrayoperationscreator(lib.extend, lib.readPropertyFromDotDelimitedString, lib.isFunction, lib.Map, lib.AllexJSONizingError),
    StorageType = dataSuite.MemoryStorage; //dataSuite.MemoryListStorage;

  //q.longStackSupport = true;
  function factoryCreator(parentFactory) {
    return {
      'service': require('./users/serviceusercreator')(execlib, parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib, parentFactory.get('user')) 
    };
  }

  function waitForDestruction (sink) {
    var d = q.defer();
    sink.destroyed.attach(d.resolve.bind(d, true));
    sink.destroy();
    return d.promise;
  }

  var _DDS2DSid = 0;
  function DDS2DS(directorydataserviceinstance, dataservicesink) {
    lib.Destroyable.call(this); //gotta be Destroyable, so that consumeChannel does not wrap me up
    //this.id = ++_DDS2DSid;
    //console.log('DDS2DS',this.id,'starting');
    this.dds = directorydataserviceinstance;
    this.ddsdl = this.dds.destroyed.attach(this.onDirectoryDataDead.bind(this));
    this.ds = dataservicesink;
    this.dsdl = dataservicesink.destroyed.attach(this.onDataServiceSinkDead.bind(this));
    this.ds.consumeChannel('fs',this);
  }
  lib.inherit(DDS2DS, lib.Destroyable);
  DDS2DS.prototype.__cleanUp = function () {
    //console.trace();
    //console.log('DDS2DS',this.id,'dying');
    if (this.ds) {
      this.ds.consumeChannel('fs', null);
    }
    if (this.dsdl) {
      this.dsdl.destroy();
    }
    this.dsdl = null;
    if (this.ddsdl) {
      this.ddsdl.destroy();
    }
    this.ddsdl = null;
    this.ds = null;
    this.dds = null;
  };
  DDS2DS.prototype.onDirectoryDataDead = function () {
    //console.log('DDS2DS',this.id,'found DirectoryDataService instance dead');
    this.destroy();
  };
  DDS2DS.prototype.onDataServiceSinkDead = function () {
    //console.log('DDS2DS',this.id,'found DirectoryService sink dead');
    this.destroy();
  };
  DDS2DS.prototype.onStream = function (item) {
    //console.log('DDS2DS',this.id,'queueing');
    this.dds.queueTraversal(this.ds).done(
      null,
      this.destroy.bind(this)
    );
  };

  function DirectoryDataService(prophash) {
    /* not obligatory any more, setDirectorySink is an alternative
    if(!(prophash && prophash.path)){
      throw new lib.Error('DIRECTORY_DATA_SERVICE_NEEDS_A_PATH','DirectoryDataService misses the propertyhash.path field');
    }
    */
    ParentService.call(this, prophash);
    this.path = prophash.path;
    this.parserinfo = prophash.parserinfo; //{modulename: '...', propertyhash: {...}}
    this.files = prophash.files;
    this.metastats = prophash.metastats;
    this.filetypes = prophash.filetypes;
    this.needparsing = prophash.needparsing;
    this.dirUserSuperSink = null;
    this.dirUserSink = null;
    this.dirUserSinkDestructionListener = null;
    this.traversalDefer = null;
  }
  ParentService.inherit(DirectoryDataService, factoryCreator, require('./storagedescriptor'));
  DirectoryDataService.prototype.__cleanUp = function() {
    if (this.traversalDefer) {
      this.traversalDefer.resolve(true);
    }
    this.traversalDefer = null;
    this.purgeDirUserSinkDestructionListener();
    if(this.dirUserSink){
      this.dirUserSink.destroy();
    }
    this.dirUserSink = null;
    if(this.dirUserSuperSink){
      this.dirUserSuperSink.destroy();
    }
    this.dirUserSuperSink = null;
    this.filetypes = null;
    this.metastats = null;
    this.files = null;
    this.parserinfo = null;
    this.path = null;
    ParentService.prototype.__cleanUp.call(this);
  };
  DirectoryDataService.prototype.createStorage = function(storagedescriptor) {
    //return ParentService.prototype.createStorage.call(this, storagedescriptor);
    return new StorageType(storagedescriptor);
  };
  DirectoryDataService.prototype.onSuperSink = function (supersink) {
    ParentService.prototype.onSuperSink.apply(this,arguments);
    if (this.path) {
      this.generateDirectoryRecords().done(
        null,
        this.close.bind(this)
      );
    }
  };
  DirectoryDataService.prototype.generateDirectoryRecords = function (sink) {
    if (!sink) {
      if (this.path) {
        //console.log('"standalone" mode!');
        return this.startSubServiceStatically(this.directoryServiceModuleName,'directoryservice',{path:this.path}).then(
          this.onDirectorySubServiceSuperSink.bind(this)
        );
      } else {
        return q.reject(new lib.Error('NO_SINK_NO_PATH', 'DirectoryDataService needs either a sink or a path'));
      }
    } else {
      return this.onDirectorySubService(sink);
    }
  };
  DirectoryDataService.prototype.onDirectorySubServiceSuperSink = function (sink) {
    if (!sink) {
      return q(false);
    }
    if (this.dirUserSuperSink && this.dirUserSuperSink.destroyed) {
      return waitForDestruction(this.dirUserSuperSink).then(this.setDirectorySubServiceSuperSink.bind(this, sink));
    } else {
      return this.setDirectorySubServiceSuperSink(sink);
    }
  };
  DirectoryDataService.prototype.setDirectorySubServiceSuperSink = function (sink) {
    this.dirUserSuperSink = sink;
    return sink.subConnect('.',{name:'user',role:'user'}).then(
      this.onDirectorySubService.bind(this)
    );
  };
  DirectoryDataService.prototype.onDirectorySubService = function (sink) {
    if (this.dirUserSink) {
      if (this.dirUserSink.destroyed) {
        //console.log(this.actualPath(), 'will wait for destruction');
        return waitForDestruction(this.dirUserSink).then(this.scanSubDirectory.bind(this, sink));
      } else {
        //console.log('this is my wreck of dirUserSink');
        //console.log(require('util').inspect(this.dirUserSink, {depth:7}));
        process.exit(5);
        return;
      }
    } else {
      return this.scanSubDirectory(sink);
    }
  };
  DirectoryDataService.prototype.scanSubDirectory = function (sink) {
    if (!this.destroyed) {
      return q(true);
    }
    if (!sink.destroyed) {
      return q(true);
    }
    this.dirUserSinkDestructionListener = sink.destroyed.attach(this.onDirUserSinkDead.bind(this));
    this.dirUserSink = sink;
    //console.log(this.actualPath(), 'will new DDS2DS');
    new DDS2DS(this, sink);
    return this.queueTraversal(sink);
  };
  DirectoryDataService.prototype.onDirUserSinkDead = function () {
    this.purgeDirUserSinkDestructionListener();
    this.dirUserSink = null;
  };
  DirectoryDataService.prototype.purgeDirUserSinkDestructionListener = function () {
    var dusdl = this.dirUserSinkDestructionListener;
    this.dirUserSinkDestructionListener = null;
    if (dusdl) {
      lib.runNext(dusdl.destroy.bind(dusdl), 100);
    }
  };
  DirectoryDataService.prototype.queueTraversal = function (sink) {
    if (!this.destroyed) {
      return q.reject(new lib.Error('ALREADY_DEAD', 'Cannot do things while dead'));
    }
    if (this.traversalDefer) {
      return this.traversalDefer.promise;
    }
    this.traversalDefer = q.defer();
    this.traversalDefer.promise.then(
      this.afterTraversal.bind(this),
      this.onTraversalFail.bind(this)
    );
    this.doTheTraversal(sink, this.traversalDefer);
    return this.traversalDefer.promise;
  };
  DirectoryDataService.prototype.afterTraversal = function () {
    this.traversalDefer = null;
  };
  DirectoryDataService.prototype.onTraversalFail = function (reason) {
    var t;
    if (this.dirUserSink) {
      this.dirUserSink.destroy();
    }
    this.dirUserSink = null;
    if (this.dirUserSuperSink) {
      this.dirUserSuperSink.destroy();
    }
    this.dirUserSuperSink = null;
    this.traversalDefer = null;
  };
  function fileFieldNameExtractor(names, fld) {
    if (fld && fld.name && fld.isfilestat) {
      names.push(fld.name);
    }
    return names;
  }
  function fileMetaFieldNameExtractor(names, fld) {
    if (fld && fld.name && fld.ismeta) {
      if (fld.src) {
        names.push({src:fld.src, dest:fld.name});
      } else {
        names.push(fld.name);
      }
    }
    return names;
  }
  DirectoryDataService.prototype.doTheTraversal = function (sink, defer) {
    if (!(sink && sink.destroyed)) { //this one's dead
      //console.log('no sink to call traversal upon');
      if (defer) {
        defer.resolve('ok');
      }
      return;
    }
    //console.trace();
    //console.log(this.files, this.parserinfo, 'starts delete');
    this.data.delete();
    //console.log(this.files, 'ends delete');
    var filestats, metastats, to;
    filestats = this.storageDescriptor.record.fields.reduce(fileFieldNameExtractor, []);
    metastats = this.storageDescriptor.record.fields.reduce(fileMetaFieldNameExtractor, []);
    if (lib.isArray(this.metastats)) {
      arrayOperations.appendNonExistingItems(metastats, this.metastats);
    }
    to = {
      filestats: filestats
    };
    if (this.files) {
      to.files = this.files;
    }
    if (this.parserinfo) {
      to.filecontents = this.parserinfo;
    }
    if (metastats.length) {
      to.metastats = metastats;
    }
    if (this.filetypes) {
      to.filetypes = this.filetypes;
    }
    if (this.needparsing) {
      to.needparsing = this.needparsing;
    }
    //console.log('traversing with', require('util').inspect(to, {depth:null}));
    //defer.promise.then(console.log.bind(console, 'traverse done'));
    this.filetypes = ['d'];
    sink.call('traverse','',to).done(
      defer ? defer.resolve.bind(defer, 'ok') : null,
      defer ? defer.reject.bind(defer) : console.error.bind(console, 'traverse error'),
      this.onDirectoryDataRecord.bind(this)
    );
  };
  DirectoryDataService.prototype.onDirectoryDataRecord = function (record) {
    //console.log('record', record);
    record = this.makeUpRecord(record);
    if (this.data) {
      this.data.create(record);
      this.onRecordCreated(record);
    } else {
      console.log('too late for', record);
    }
  };
  DirectoryDataService.prototype.onRecordCreated = function (record) {
  };
  DirectoryDataService.prototype.actualPath = function () {
    return this.path ? this.path : process.cwd();
  };
  DirectoryDataService.prototype.makeUpRecord = function (record) {
    return record;
  }
  DirectoryDataService.prototype.directoryServiceModuleName = 'allex_directoryservice';
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
