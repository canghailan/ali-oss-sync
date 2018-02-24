const util = require("util");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const url = require("url");
const oss = require("ali-oss").Wrapper;

const promisify = util.promisify || function (fn, receiver) {
  return (...args) => {
    return new Promise((resolve, reject) => {
      fn.apply(receiver, [...args, (err, res) => {
        return err ? reject(err) : resolve(res);
      }]);
    });
  };
};

fs.statAsync = promisify(fs.stat, fs);
fs.readdirAsync = promisify(fs.readdir, fs);

const auth = /([^:]+):(.+)/;
const host = /([^.]+).(.+)/;

module.exports = async(...args) => {
  let context = {};
  if (args.length === 1) {
    let [option] = args;
    Object.assign(context, option);
  } else {
    let [source, target, headers] = args;
    context.source = source;
    context.target = target;
    context.headers = headers;
  }

  return sync(init(context));
};

function init(context) {
  let targetUri = url.parse(context.target);
  let [, username, password] = targetUri.auth.match(auth);
  let [, bucketName, endpoint] = targetUri.hostname.match(host);
  context.targetBucket = oss({
    accessKeyId: username,
    accessKeySecret: password,
    bucket: bucketName,
    endpoint: endpoint
  });
  context.targetPrefix = targetUri.pathname.substring(1);

  return context;
}

async function sync(context) {
  let files = await listFiles(context.source);
  let objects = await listObjects(context.targetBucket, context.targetPrefix);
  let objectIndex = newObjectIndex(context.targetPrefix, objects);
  return apply(context, diff(files, objectIndex));
}

async function apply(context, diff) {
  let r = diff.map(async action => {
    switch (action.type) {
      case "+":
      case "*":
        {
          await context.targetBucket.put(context.targetPrefix + action.key, path.join(context.source, action.key), {
            headers: context.headers
          });
          console.log(`${action.type} ${action.key}`);
          return action;
        }
      case "-":
        {
          await context.targetBucket.delete(context.targetPrefix + action.key);
          console.log(`${action.type} ${action.key}`);
          return action;
        }
      case "=":
        {
          console.log(`${action.type} ${action.key}`);
          return action;
        }
      default:
        {
          console.error(action);
          return action;
        }
    }
  });
  return Promise.all(r);
}

function diff(files, objectIndex) {
  let list = files.map(file => {
    let object = objectIndex.get(file.key);
    if (object) {
      objectIndex.delete(file.key);
      if (object.etag.slice(1, -1).toLowerCase() === file.md5) {
        return {
          type: "=",
          key: file.key,
          md5: file.md5
        };
      } else {
        return {
          type: "*",
          key: file.key,
          md5: file.md5
        };
      }
    } else {
      return {
        type: "+",
        key: file.key,
        md5: file.md5
      };
    }
  });
  objectIndex.forEach((object, key) => {
    list.push({
      type: "-",
      key: key
    });
  });
  return list;
}

function newObjectIndex(prefix, objects) {
  let index = new Map();
  objects.forEach(object => {
    index.set(object.name.substring(prefix.length), object);
  });
  return index;
}

async function listObjects(bucket, prefix, marker) {
  let r = await bucket.list({
    prefix: prefix,
    "max-keys": 1000,
    marker: marker
  });
  let objects = r.objects || [];
  if (r.nextMarker == null) {
    return objects;
  } else {
    return objects.concat(await listObjects(bucket, prefix, r.nextMarker));
  }
}

async function listFiles(start, rel) {
  let p = rel ? path.join(start, rel) : start;
  let s = await fs.statAsync(p);
  if (s.isDirectory()) {
    let files = await fs.readdirAsync(p);
    return files
      .map(file => {
        return listFiles(start, rel ? rel + "/" + file : file);
      })
      .reduce(async(a1, a2) => {
        let aa1 = await a1;
        let aa2 = await a2;
        return aa1.concat(aa2);
      });
  } else {
    return [{
      key: rel,
      md5: await md5(p)
    }];
  }
}

function md5(file) {
  return new Promise((resolve, reject) => {
    let hash = crypto.createHash("md5");
    let stream = fs.createReadStream(file);
    stream.on("data", (data) => {
      hash.update(data);
    });
    stream.on("end", () => {
      resolve(hash.digest("hex"));
    });
  });
}