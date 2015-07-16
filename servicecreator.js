function createBaseService(execlib, ParentServicePack) {
  'use strict';
  var lib = execlib.lib,
    ParentService = ParentServicePack.Service,
    dataSuite = execlib.dataSuite,
    MemoryStorage = dataSuite.MemoryStorage;

  function factoryCreator(parentFactory) {
    return {
      'service': require('./users/serviceusercreator')(execlib, parentFactory.get('service')),
      'user': require('./users/usercreator')(execlib, parentFactory.get('user')) 
    };
  }

  function BaseService(prophash) {
    if(!(prophash && prophash.path)){
      throw new lib.Error('DIRECTORY_DATA_SERVICE_NEEDS_A_PATH','DirectoryDataService misses the propertyhash.path field');
    }
    ParentService.call(this, prophash);
    this.path = prophash.path;
    this.parserinfo = prophash.parserinfo; //{modulename: '...', propertyhash: {...}}
    this.supersink = null;
    this.dirUserSink = null;
  }
  ParentService.inherit(BaseService, factoryCreator, require('./storagedescriptor'));
  BaseService.prototype.__cleanUp = function() {
    if(this.dirUserSink){
      this.dirUserSink.destroy();
    }
    this.dirUserSink = null;
    this.supersink = null;
    this.parserinfo = null;
    this.path = null;
    ParentService.prototype.__cleanUp.call(this);
  };
  BaseService.prototype.createStorage = function(storagedescriptor) {
    //return ParentService.prototype.createStorage.call(this, storagedescriptor);
    return new MemoryStorage(storagedescriptor);
  };
  BaseService.prototype.onSuperSink = function (supersink) {
    this.supersink = supersink;
    ParentService.prototype.onSuperSink.apply(this,arguments);
    this.generateDirectoryRecords();
  };
  BaseService.prototype.generateDirectoryRecords = function () {
    this.supersink.call('delete');
    this.startSubServiceStatically('allex_directoryservice','directoryservice',{path:this.path}).done(
      this.onDirectorySubServiceSuperSink.bind(this)
    );
  };
  BaseService.prototype.onDirectorySubServiceSuperSink = function (sink) {
    sink.subConnect('.',{name:'user',role:'user'}).done(
      this.onDirectorySubService.bind(this)
    );
  };
  BaseService.prototype.onDirectorySubService = function (sink) {
    sink.call('traverse','',{
      filestats: this.storageDescriptor.record.fields.map(function(fld){return fld.name;}),
      filecontents: this.parserinfo ? this.parserinfo : null
    }).done(
      null,
      console.error.bind(console,'traverse error:'),
      this.supersink.call.bind(this.supersink,'create')
    );
  };
  return BaseService;
}

module.exports = createBaseService;
