function createDirectoryDataService(execlib, ParentServicePack) {
  'use strict';
  var lib = execlib.lib,
    q = lib.q,
    ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite,
    StorageType = dataSuite.MemoryStorage; //dataSuite.MemoryListStorage;

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
    console.log('DDS2DS',this.id,'queueing');
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
    this.metastats = prophash.metastats;
    this.filetypes = prophash.filetypes;
    this.needparsing = prophash.needparsing;
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
    this.generateDirectoryRecords();
  };
  DirectoryDataService.prototype.generateDirectoryRecords = function (sink, defer) {
    defer = defer || q.defer();
    //console.log('generating records', sink? 'with' : 'without', 'sink');
    if (!sink) {
      if (this.path) {
        //console.log('"standalone" mode!');
        this.startSubServiceStatically('allex_directoryservice','directoryservice',{path:this.path}).done(
          this.onDirectorySubServiceSuperSink.bind(this),
          console.error.bind(console, 'startSubServiceStatically error')
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
      this.onDirectorySubService.bind(this),
      console.error.bind(console, 'no subconnect to self')
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
        this.afterTraversal.bind(this),
        console.error.bind(console, 'error in traversal')
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
    sink.call('traverse','',to).done(
      defer ? defer.resolve.bind(defer, 'ok') : null,
      /*
      function (){
        console.log('traverse done', scountobj.cnt);
        if (defer) {
          defer.resolve('ok');
        }
      },
      */
      defer ? defer.reject.bind(defer) : console.error.bind(console, 'traverse error'),
      this.onDirectoryDataRecord.bind(this)
      /*
      this.data.create.bind(this.data)
      this.supersink.call.bind(this.supersink,'create')
      function (item) {
        //console.log(item);
        scountobj.cnt++;
      }
      */
    );
  };
  DirectoryDataService.prototype.onDirectoryDataRecord = function (record) {
    if (this.data) {
      this.data.create(record);
    } else {
      console.log('too late for', record);
    }
  };
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
