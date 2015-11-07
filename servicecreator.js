function createDirectoryDataService(execlib, ParentServicePack) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite,
    StorageType = dataSuite.MemoryStorage; //dataSuite.MemoryListStorage;

  //q.longStackSupport = true;
  function factoryCreator(parentFactory) {
    return {
      'service': require('./users/serviceusercreator')(execlib, parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib, parentFactory.get('user')) 
    };
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
    console.log('DDS2DS',this.id,'queueing');
    this.dds.queueTraversal(this.ds).done(
      null,
      //this.destroy.bind(this)
      console.error.bind(console, 'sad bi ja trebalo da se ubijem')
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
    this.traversalDefer = null;
    this.traversals = new lib.Fifo();
  }
  ParentService.inherit(DirectoryDataService, factoryCreator, require('./storagedescriptor'));
  DirectoryDataService.prototype.__cleanUp = function() {
    if (this.traversals) {
      this.traversals.destroy();
    }
    this.traversals = null;
    if (this.traversalDefer) {
      this.traversalDefer.resolve(true);
    }
    this.traversalDefer = null;
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
  DirectoryDataService.prototype.generateDirectoryRecords = function (sink, defer) {
    defer = defer || q.defer();
    if (!sink) {
      if (this.path) {
        //console.log('"standalone" mode!');
        this.startSubServiceStatically('allex_directoryservice','directoryservice',{path:this.path}).done(
          this.onDirectorySubServiceSuperSink.bind(this, defer),
          console.error.bind(console, 'startSubServiceStatically error')
        );
      } else {
        defer.reject(new lib.Error('NO_SINK_NO_PATH', 'DirectoryDataService needs either a sink or a path'));
      }
    } else {
      this.onDirectorySubService(defer, sink);
    }
    return defer.promise;
  };
  DirectoryDataService.prototype.onDirectorySubServiceSuperSink = function (defer, sink) {
    if (this.dirUserSuperSink) {
      this.dirUserSuperSink.destroy();
    }
    this.dirUserSuperSink = sink;
    sink.subConnect('.',{name:'user',role:'user'}).done(
      this.onDirectorySubService.bind(this, defer),
      console.error.bind(console, 'no subconnect to self')
    );
  };
  DirectoryDataService.prototype.onDirectorySubService = function (defer, sink) {
    if (!(defer && defer.promise)) {
      console.trace();
      console.error('NO DEFER?!');
      return;
    }
    if (this.dirUserSink) {
      this.dirUserSink.destroy();
    }
    this.dirUserSink = sink;
    new DDS2DS(this, sink);
    this.queueTraversal(sink, defer);
    return defer.promise;
  };
  DirectoryDataService.prototype.queueTraversal = function (sink, defer) {
    defer = defer || q.defer();
    if (!this.destroyed) {
      defer.reject(new lib.Error('ALREADY_DEAD', 'Cannot do things while dead'));
      return defer.promise;
    }
    if (this.traversalDefer) {
      this.traversals.push([sink,defer]);
      return defer.promise;
    }
    this.traversalDefer = defer;
    this.doTheTraversal(sink, this.traversalDefer);
    this.traversalDefer.promise.then(
        this.afterTraversal.bind(this),
        this.onTraversalFail.bind(this)
    );
    return this.traversalDefer.promise;
  };
  DirectoryDataService.prototype.afterTraversal = function () {
    if (!this.traversals) { //me ded already
      return;
    }
    this.traversalDefer = null;
    if (this.traversals.length) {
      this.queueTraversal.apply(this, this.traversals.pop());
    }
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
    while (this.traversals.length) {
      t = this.traversals.pop();
      if (t[1]) {
        t[1].reject(reason);
      }
    }
  };
  function fileFieldNameExtractor(names, fld) {
    if (!(fld && fld.name)) {
      return;
    }
    if (!fld.ismeta) {
      names.push(fld.name);
    }
  }
  function fileMetaFieldNameExtractor(names, fld) {
    if (!(fld && fld.name)) {
      return;
    }
    if (fld.ismeta) {
      names.push(fld.name);
    }
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
    var filestats = [], metastats = [], to;
    this.storageDescriptor.record.fields.forEach(fileFieldNameExtractor.bind(null, filestats));
    this.storageDescriptor.record.fields.forEach(fileMetaFieldNameExtractor.bind(null, metastats));
    if (lib.isArray(this.metastats)) {
      lib.arryOperations.appendNonExistingItems(metastats, this.metastats);
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
    sink.call('traverse','',to).done(
      defer ? defer.resolve.bind(defer, 'ok') : null,
      defer ? defer.reject.bind(defer) : console.error.bind(console, 'traverse error'),
      this.onDirectoryDataRecord.bind(this)
    );
  };
  DirectoryDataService.prototype.onDirectoryDataRecord = function (record) {
    //console.log('record', record);
    if (this.data) {
      this.data.create(record);
    } else {
      console.log('too late for', record);
    }
  };
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
