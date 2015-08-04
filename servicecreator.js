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

  function DirectoryDataService(prophash) {
    /* not obligatory any more, setDirectorySink is an alternative
    if(!(prophash && prophash.path)){
      throw new lib.Error('DIRECTORY_DATA_SERVICE_NEEDS_A_PATH','DirectoryDataService misses the propertyhash.path field');
    }
    */
    ParentService.call(this, prophash);
    this.path = prophash.path;
    this.parserinfo = prophash.parserinfo; //{modulename: '...', propertyhash: {...}}
    this.supersink = null;
    this.dirUserSink = null;
  }
  ParentService.inherit(DirectoryDataService, factoryCreator, require('./storagedescriptor'));
  DirectoryDataService.prototype.__cleanUp = function() {
    if(this.dirUserSink){
      this.dirUserSink.destroy();
    }
    this.dirUserSink = null;
    this.supersink = null;
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
        //problem...
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
    sink.consumeChannel('fs', this.doTheTraversal.bind(this, sink, defer));
    return defer.promise;
  };
  DirectoryDataService.prototype.doTheTraversal = function (sink, defer) {
    defer = defer || q.defer();
    this.supersink.call('delete');
    sink.call('traverse','',{
      filestats: this.storageDescriptor.record.fields.map(function(fld){return fld.name;}),
      filecontents: this.parserinfo ? this.parserinfo : null
    }).done(
      defer.resolve.bind(defer, 'ok'),
      //console.error.bind(console,'traverse error:'),
      defer.reject.bind(defer),
      this.supersink.call.bind(this.supersink,'create')
    );
    return defer.promise;
  };
  return DirectoryDataService;
}

module.exports = createDirectoryDataService;
