// Copyright 2012-2016 by Frank Trampe.
// All rights reserved except as granted by an accompanying license.

var multer = require('multer');
var multipartMiddleware = multer();
var dauria = require('dauria');
// var fs = require('fs');
var request = require('request');
var ReadWriteLock = require('rwlock');
var tmp = require('tmp');

function promisifyFlex(fsfunc, datapos, thisC, args) {
	// This allows us to wrap simple functions with callbacks into promises.
	return new Promise(function (resolve, reject) {
		// We copy the argument list.
		var fullArgs = args.slice();
		// We push a function onto the argument list according to the callback format.
		if (datapos == 0) {
			fullArgs.push(function (err) {
				if (err) reject(err);
				else resolve(0);
			});
		} else if (datapos == 1) {
			fullArgs.push(function (data, err) {
				if (err) reject(err);
				else resolve(data);
			});
		} else if (datapos == -1) {
			fullArgs.push(function (err, data) {
				if (err) reject(err);
				else resolve(data);
			});
		} else if (datapos == 2) {
			// We assume that err is at 0 in this case and ignore 1.
			fullArgs.push(function (err, discard, data) {
				if (err) reject(err);
				else resolve(data);
			});
		}
		// We call the function.
		fsfunc.apply(thisC, fullArgs);
	});
}

var fsp = {
	writeFile: function(path, data) {
		return promisifyFlex(fs.writeFile, 0, fs, [path, data]);
	},
	readFile: function(path) {
		return promisifyFlex(fs.readFile, -1, fs, [path]);
	},
	readdir: function(path, options) {
		return promisifyFlex(fs.readdir, -1, fs, [path]); // There is an optional second argument, an object of options, but it is not supported on old fs.
	},
	rename: function(oldPath, newPath) {
		return promisifyFlex(fs.rename, 0, fs, [oldPath, newPath]);
	},
	access: function(path, flags) {
		return promisifyFlex(fs.access, 0, fs, [path, flags]);
	},
	constants: (fs.constants || fs)
};

var requestp = {
	get: function(path) {
		return promisifyFlex(request.get, 2, request, [path]);
	}
};

function stringFromDataURI(iv) {
	var decoded = dauria.parseDataURI(iv);
	if ('text' in decoded) return decoded.text;
	return null;
}

function stringFromBuffer(iv, enc) {
	var oencode = 'utf8'; // A sane default.
	if (typeof(enc) == 'string' && enc != "") {
		switch (enc) {
			case 'US-ASCII':
			case 'ascii':
			case 'ASCII':
			case 'us-ascii':
			case '7bit':
				oencode = 'ascii';
				break;
			case 'utf8':
			case 'UTF8':
				oencode = 'utf8';
				break;
			default:
				break;
		}
	}
	var ostring1 = iv.toString(oencode);
	return ostring1;
}

var writeTempFile = function(inBuffer) {
	// This makes a temporary file, dumps the contents of the buffer into that file, and returns the file path.
	return new Promise(
		function (resolve, reject) {
			console.log("Trying tmp.");
			tmp.file(function (err, path, fd, cleanupCallback) {
				if (err) reject(new Error("Temporary file creation failed."));
				console.log("Made file.");
				fs.write(fd, inBuffer, 0, inBuffer.length, 0, function (err2, written, buffer) {
					fs.close(fd, function (err3) {
						if (err3) reject(new Error("Closing the temporary file failed."));
						if (err2 || written != buffer.length) reject(new Error("Writing to the temporary file failed."));
						console.log("Wrote file.");
						resolve(path);
					});
				});
			});
		}
	);
}
var makeTempFile = function() {
	// This makes a temporary file and returns the file path.
	return new Promise(
		function (resolve, reject) {
			console.log("Trying tmp.");
			tmp.file(function (err, path, fd, cleanupCallback) {
				if (err) reject(new Error("Temporary file creation failed."));
				console.log("Made file.");
				fs.close(fd, function (err2) {
					if (err2) reject(new Error("Closing the temporary file failed."));
					resolve(path);
				});
			});
		}
	);
}

