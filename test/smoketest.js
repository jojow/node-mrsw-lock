var uuid = require('uuid');
var chai = require('chai');
var expect = chai.expect;

var MRSWLock = require('../');
var lock = new MRSWLock();
var config = {};



describe('MRSWLock smoke test', function() {
  describe('#readLock()', function() {
    it('get two read locks on the same object', function(done) {
      var id = uuid.v4();

      lock.readLock(id, function(err, token) {
        expect(err).to.not.exist;
        expect(token).to.exist;

        lock.readLock(id, function(err, token) {
          expect(err).to.not.exist;
          expect(token).to.exist;

          done();
        });
      });
    });
  });

  describe('#readRelease()', function() {
    it('get read lock, release read lock, and get write lock', function(done) {
      var id = uuid.v4();

      lock.readLock(id, function(err, token) {
        expect(err).to.not.exist;
        expect(token).to.exist;

        lock.readRelease(id, token, function(err) {
          expect(err).to.not.exist;

          lock.writeLock(id, function(err, token) {
            expect(err).to.not.exist;
            expect(token).to.exist;

            done();
          });
        });
      });
    });
  });

  describe('#writeLock()', function() {
    it('get read lock and try to get write lock', function(done) {
      var id = uuid.v4();

      lock.readLock(id, function(err, token) {
        expect(err).to.not.exist;
        expect(token).to.exist;

        lock.writeLock(id, function(err, token) {
          expect(err).to.exist;
          expect(token).to.not.exist;

          done();
        });
      });
    });
  });

  describe('#writeRelease()', function() {
    it('get write lock, release write lock, and get read lock', function(done) {
      var id = uuid.v4();

      lock.writeLock(id, function(err, token) {
        expect(err).to.not.exist;
        expect(token).to.exist;

        lock.writeRelease(id, token, function(err) {
          expect(err).to.not.exist;

          lock.readLock(id, function(err, token) {
            expect(err).to.not.exist;
            expect(token).to.exist;

            done();
          });
        });
      });
    });
  });
});
