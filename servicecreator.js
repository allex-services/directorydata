function createDirectoryDataService(execlib, ParentServicePack) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite,
    MemoryStorage = dataSuite.MemoryStorage;

  function factoryCreator(parentFactory) {
    return {
      'service': require('./users/serviceusercreator')(execlib, parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib, parentFactory.get('user')) 
    };
  }

  var _DDS2DSid = 0;
  function DDS2DS(directorydataserviceinstance, dataservicesink) {
    lib.Destroyable.call(this); //gotta be Destroyable, so that consumeChannel does not wrap me up
    this.id = ++_DDS2DSid;
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
    this.ds.consumeChannel('fs', null);
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
    this.dds.queueTraversal(this.ds);
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
    this.supersink = null;
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
    this.supersink = null;
    this.files = null;
    this.parserinfo = null;
    this.path = null;
    ParentService.prototype.__cleanUp.call(this);
  };
  DirectoryDataService.prototype.createStorage = function(storagedescriptor) {
    //return ParentService.prototype.createStorage.call(this, storagedescriptor);
    return new MemoryStorage(storagedescriptor);
  };
  DirectoryDataService.prototype.onSuperSink = function (supersink) {
    this.supersink = supersink;
    ParentService.prototype.onSuperSink.apply(this,arguments);
    this.generateDirectoryRecords();
  };
  DirectoryDataService.prototype.generateDirectoryRecords = function (sink, defer) {
    defer = defer || q.defer();
    if (!sink) {
      if (this.path) {
        this.startSubServiceStatically('allex_directoryservice','directoryservice',{path:this.path}).done(
          this.onDirectorySubServiceSuperSink.bind(this)
        );
      } else {
        //console.error('No sink, no path...');
        defer.reject(new lib.Error('NO_SINK_NO_PATH', 'DirectoryDataService needs either a sink or a path'));
      }
    } else {
      this.onDirectorySubService(sink, defer);
    }
    return defer.promise;
  };
  DirectoryDataService.prototype.onDirectorySubServiceSuperSink = function (sink) {
    sink.subConnect('.',{name:'user',role:'user'}).done(
      this.onDirectorySubService.bind(this)
    );
  };
  DirectoryDataService.prototype.onDirectorySubService = function (sink, defer) {
    defer = defer || q.defer();
    new DDS2DS(this, sink);
    this.doTheTraversal(sink, defer);
    return defer.promise;
  };
  DirectoryDataService.prototype.queueTraversal = function (sink) {
    if (!this.destroyed) {
      //console.log('cannot queueTraversal, me ded');
      return;
    }
    if (this.traversalDefer) {
      this.traversals.push(sink);
      return;
    }
    this.traversalDefer = q.defer();
    this.doTheTraversal(sink, this.traversalDefer);
    this.traversalDefer.promise.done(
        this.afterTraversal.bind(this)
    );
  };
  DirectoryDataService.prototype.afterTraversal = function () {
    if (!this.traversals) { //me ded already
      return;
    }
    this.traversalDefer = null;
    if (this.traversals.length) {
      this.queueTraversal(this.traversals.pop());
    }
  };
  DirectoryDataService.prototype.doTheTraversal = function (sink, defer) {
    if (!(sink && sink.destroyed)) { //this one's dead
      //console.log('no sink to call traverasl upon');
      if (defer) {
        defer.resolve('ok');
      }
      return;
    }
    this.supersink.call('delete');
    //var ss = this.supersink;
    var to = {
      filestats: this.storageDescriptor.record.fields.map(function(fld){return fld.name;}),
      files: this.files
    };
    if (this.parserinfo) {
      to.filecontents = this.parserinfo;
    }
    sink.call('traverse','',to).done(
      defer ? defer.resolve.bind(defer, 'ok') : null,
      defer ? defer.reject.bind(defer) : null,
      this.supersink.call.bind(this.supersink,'create')
      /*
      function (item) {
        console.log(item);
        ss.call('create', item);
      }
      */
    );
  };
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
