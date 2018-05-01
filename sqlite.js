const ptool = require('path');
var sqlite3 = require('sqlite3');

function fnDbOps(fname, cb) {
	var t = this;
	t.debug = 1;
	t.status = 0;
	var fnClose = function(cb) {
		if(typeof cb!='function') cb = function() {};
		if(t.status=1) db.close(cb);
	}
	let fpath = fname;
	if(!ptool.isAbsolute(fpath))
		fpath = ptool.join(__dirname, fname);

	var db = new sqlite3.Database(fname, function(e) {
		if(e==null) t.status = 1;
		else {
			t.status = -1;
			t.error = e;
			console.log(e);
		}
		db.configure('trace', function(s) {
			if(t.debug) console.log(s);
		});
		process.on('exit', fnClose);
		process.on('SIGINT', fnClose);
		process.on('uncaughtException', fnClose);
		if(typeof cb=='function') cb();
	});
	t.db = db;

	/*******************************************************/
	t.run = (sql,params)=> {
		let asql = sql.split(/;\s*\n/g);
		let apr = [];
		for(let i=0;i<asql.length;i++){
			let s=asql[i];
			let pr = new Promise((r,e)=> {
				db.run(s, params, e=> {
					if(e==null) r();
					else e(e, s);
				});
			});
			apr.push(pr);
		}
		return Promise.all(apr);
	};
	t.runFile = fname=> {
		let fpath = fname;
		if(!ptool.isAbsolute(fpath))
			fpath = ptool.join(__dirname, fname);
		let sql = fs.readFileSync(fpath).toString();
		return t.run(sql);
	};

	t.all = (sql,params)=> new Promise((r,e)=> {
		db.all(sql, params, (err,data)=> {
			if(err!=null) e(err, sql, params);
			else r(data);
		});
	});
	t.each = (sql,params, cb)=> new Promise((r,e)=> {
		db.all(sql, params, cb, (err,numrows)=> {
			if(err!=null) e(err, sql, params);
			else r(numrows);
		});
	});

	/*******************************************************/
	t.tables = ()=> {
		let pr = new Promise((r,e)=> {
			db.all("SELECT * FROM sqlite_master", (err, d)=> {
				if(err!=null) e(err);
				else r(d);
			})
		});
		return pr;
	};
	t.tstruct = (tname, detail=0)=> {
		let tstruct = null;
		if(typeof tname!=='string') {
			tstruct = tname;
			tname = tstruct.name;
		}

		let pr = new Promise((r,e)=> {
			db.all(`pragma table_info( ${tname} )`, (err, cols)=> {
				if(err!=null) e(err, tname);
				else if(detail<2) {
					let oc = {$TABLE:tname};
					cols.forEach(col=> { oc[col.name] = detail==1 ? (col.pk?"pk":col.type): null; });
					if(detail<0) r(oc);
					else r({tname, tstruct, cols:oc});
				} else r({tname, tstruct, cols});
			});
		});
		return pr;
	};
	t.new = tname=> t.tstruct(tname, -1);
	t.tkeys = (tname, detail=0)=> {
		let tstruct = null;
		if(typeof tname!=='string') {
			tstruct = tname;
			tname = tstruct.name;
		}

		let pr = new Promise((r,e)=> {
			db.all(`pragma foreign_key_list( ${tname} )`, (err, keys)=> {
				if(err!=null) e(err, tname);
				else if(detail<2) {
					let skeys = keys.map(k=> [k.from, k.table+'.'+k.to]);
					r({tname, tstruct, keys:skeys});
				} else r({tname, tstruct, keys});
			});
		});
		return pr;
	};
	t.structure = (detail=0)=> {
		let pr = t.tables().then(d=> {
			let apr = [];
			d.forEach(tbl=> {
				apr.push(t.tstruct(tbl, detail));
			});
			return Promise.all(apr);
		}).then(d=> {
			let apr = [];
			d.forEach( ({tstruct, cols})=> {
				apr.push(
					t.tkeys(tstruct, detail)
					.then(({tname, tstruct, keys})=> ({tname, tstruct, cols, keys}))
				);
			});
			return Promise.all(apr);
		}).then(d=> {
			let o = {};
			d.forEach( d=> {
				let {tname, tstruct, cols, keys} = d;
				if(detail>1) {
					o[tname] = d;
					return;
				}
				let oc = o[tname] = Object.assign({}, cols);
				if(detail>0) oc.$KEYS = keys;
			});
			return o;
		});
		return pr;
	};

	/*******************************************************/
	t.update = (data)=> t.tstruct(data.$TABLE, 1)
		.then(x=> {
			let eq=[], wh='', obj={};
			for(let k of Object.keys(x)){
				if(k[0]==='$') continue;
				if(data[k]) obj['$'+k] = data[k];
				else continue;
				if(x[k]==='pk') wh += x[k]+'=$'+x[k];
				else eq.push(x[k]+'=$'+x[k]);
			}
			if(wh==='' || eq.length==0) return false;
			let sql = `update ${data.$TABLE} set ${eq.join(',')} where ${wh}`;
			return sql;
		});
	t.insert = (data)=> t.tstruct(data.$TABLE, 1)
		.then(({cols})=> {
			let fn=[], vn=[], obj={};
			for(let k of Object.keys(cols)){
				//console.log('key', k, '|', cols[k], data[k]);
				if(k[0]==='$') continue;
				if(cols[k]==='pk') continue;
				if(data[k]) obj['$'+k] = data[k];
				else continue;
				fn.push(k);
				vn.push('$'+k);
			}
			if(fn.length==0 || vn.length==0) return false;
			let sql = `insert into ${data.$TABLE} (${fn.join(',')}) values (${vn.join(',')})`;
			//console.log(sql, obj);
			return t.run(sql, obj);
		});
	t.uinsert = (data, ucols)=> {
		let wh = [], cr=[];
		for(let c of ucols) {
			if(data[c]===undefined || data[c]===null) {
				console.log('unique check fail', c, data, ucols);
				return false;
			}
			wh.push(c+'=?');
			cr.push(data[c]);
		}
		let sql = `select * from ${data.$TABLE} where ${wh.join(' and ')}`;
		return t.all(sql, cr).then(x=> {
			if(x.length>0) {
				console.log('already exists', data, ucols);
				return false;
			}
			return t.insert(data);
		})

	}

	/*******************************************************/
	t.close = function(cb) {
		fnClose(cb);
	};
	return t;
}

module.exports = fnDbOps

