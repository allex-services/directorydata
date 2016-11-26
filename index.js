function createServicePack(execlib) {

  return {
    service: {
      dependencies: ['allex:data']
    },
    sinkmap: {
      dependencies: ['allex:data']
    }
  };
}

module.exports = createServicePack;

