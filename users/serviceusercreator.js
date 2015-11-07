function createServiceUser(execlib, ParentUser) {
  'use strict';
  if (!ParentUser) {
    ParentUser = execlib.execSuite.ServicePack.Service.prototype.userFactory.get('user');
  }

  function ServiceUser(prophash) {
    ParentUser.call(this, prophash);
  }
  ParentUser.inherit(ServiceUser, require('../methoddescriptors/serviceuser'), [/*visible state fields here*/]/*or a ctor for StateStream filter*/, require('../visiblefields/serviceuser'));
  ServiceUser.prototype.__cleanUp = function () {
    ParentUser.prototype.__cleanUp.call(this);
  };
  ServiceUser.prototype.setDirectoryServiceSink = function (sink, defer) {
    this.__service.generateDirectoryRecords(sink, defer);
    /*
    this.__service.data.delete({}).done(
      this.__service.generateDirectoryRecords.bind(this.__service, sink, defer),
      defer.reject.bind(defer)
    );
    */
  };

  return ServiceUser;
}

module.exports = createServiceUser;
