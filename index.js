const fs = require("bluebird").promisifyAll(require("fs"));
const path = require("path");
const crypto = require("crypto");
const url = require("url");
const oss = require("ali-oss").Wrapper;

const auth = /([^:]+):(.+)/;
const host = /([^.]+).(.+)/;

module.exports = async function(source, target) {
  let targetUri = url.parse(target);
  let [, username, password] = targetUri.auth.match(auth);
  let [, bucketName, endpoint] = targetUri.hostname.match(host);
  let prefix = targetUri.pathname.substring(1);
  let bucket = oss({
    accessKeyId: username,
    accessKeySecret: password,
    bucket: bucketName,
    endpoint: endpoint
  });

  return sync(bucket, prefix, source);
};

async function sync(bucket, prefix, directory) {
  return apply(
    bucket,
    prefix,
    directory,
    diff(prefix, await listFiles(directory), await listObjects(bucket, prefix))
  );
}

function diff(prefix, files, objects) {
  let index = new Map();
  objects.forEach(object => {
    index.set(object.name.substring(prefix.length), object);
  });
  let list = files.map(file => {
    let object = index.get(file.key);
    if (object) {
      index.delete(file.key);
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
  index.forEach((object, key) => {
    list.push({
      type: "-",
      key: key
    });
  });
  return list;
}

async function apply(bucket, prefix, directory, diff) {
  let r = diff.map(async cmd => {
    switch (cmd.type) {
      case "+":
      case "*": {
        await bucket.put(prefix + cmd.key, path.join(directory, cmd.key));
        console.log(`${cmd.type} ${cmd.key}`);
        return cmd;
      }
      case "-": {
        await bucket.delete(prefix + cmd.key);
        console.log(`${cmd.type} ${cmd.key}`);
        return cmd;
      }
      case "=": {
        console.log(`${cmd.type} ${cmd.key}`);
        return cmd;
      }
      default: {
        console.error(cmd);
        return cmd;
      }
    }
  });
  return Promise.all(r);
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
      .reduce(async (a1, a2) => {
        let aa1 = await a1;
        let aa2 = await a2;
        return aa1.concat(aa2);
      });
  } else {
    return [
      {
        key: rel,
        md5: await md5(p)
      }
    ];
  }
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
    return objects.concat(listObjects(bucket, prefix, r.nextMarker));
  }
}

function md5(file) {
  return new Promise(function(resolve, reject) {
    let hash = crypto.createHash("md5");
    let stream = fs.createReadStream(file);
    stream.on("data", function(data) {
      hash.update(data);
    });
    stream.on("end", function() {
      resolve(hash.digest("hex"));
    });
  });
}
