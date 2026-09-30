const fs = require("fs");
const { join } = require("path");

function read(name) {
  return fs.readFileSync(join(__dirname, name));
}

class Cache {
  #secret = 1;
  get size() {
    return this.map.size;
  }
  clear() {
    this.map = new Map();
    read("x");
  }
}

const memo = function memoize(fn) {
  return fn();
};

module.exports = { read, Cache, memo };
