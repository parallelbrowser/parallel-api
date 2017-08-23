const InjestDB = require('scratch-db-test')
const coerce = require('./lib/coerce')

// exported api
// =

exports.open = async function (userArchive) {
  // setup the archive
  var db = new InjestDB('parallel:' + (userArchive ? userArchive.url : 'cache'))
  db.schema({
    version: 1,
    profile: {
      singular: true,
      index: ['*followUrls'],
      validator: record => ({
        name: coerce.string(record.name),
        bio: coerce.string(record.bio),
        avatar: coerce.path(record.avatar),
        follows: coerce.arrayOfFollows(record.follows),
        followUrls: coerce.arrayOfFollows(record.follows).map(f => f.url),
        gizmoURLs: coerce.arrayOfGizmoURLs(record.gizmoURLs)
      })
    },
    broadcasts: {
      primaryKey: 'createdAt',
      index: ['createdAt', '_origin+createdAt', 'threadRoot', 'threadParent'],
      validator: record => ({
        text: coerce.string(record.text),
        threadRoot: coerce.datUrl(record.threadRoot),
        threadParent: coerce.datUrl(record.threadParent),
        createdAt: coerce.number(record.createdAt, {required: true}),
        receivedAt: Date.now()
      })
    },
    votes: {
      primaryKey: 'subject',
      index: ['subject'],
      validator: record => ({
        subject: coerce.voteSubject(coerce.datUrl(record.subject), {required: true}),
        vote: coerce.vote(record.vote),
        createdAt: coerce.number(record.createdAt, {required: true})
      })
    },

    // TCW -- added prescript schema

    gizmos: {
      primaryKey: 'createdAt',
      index: ['createdAt', '_origin+createdAt', '_origin+gizmoOriginURL'],
      validator: record => ({
        gizmoOriginURL: coerce.string(record.gizmoOriginURL),
        gizmoOriginArchive: coerce.string(record.gizmoOriginArchive),
        gizmoOriginAuthor: coerce.string(record.gizmoOriginAuthor),
        gizmoName: coerce.string(record.gizmoName),
        gizmoDescription: coerce.string(record.gizmoDescription),
        gizmoDocs: coerce.string(record.gizmoDocs),
        gizmoJS: coerce.string(record.gizmoJS),
        createdAt: coerce.number(record.createdAt, {required: true}),
        receivedAt: Date.now()
      })
    },

    postscripts: {
      primaryKey: 'createdAt',
      index: ['createdAt', '_origin+createdAt'],
      validator: record => ({
        postscriptJS: coerce.string(record.postscriptJS),
        postscriptHTTP: coerce.string(record.postscriptHTTP),
        postscriptInfo: coerce.string(record.postscriptInfo),
        gizmoOriginArchive: coerce.string(record.gizmoOriginArchive),
        gizmoOriginURL: coerce.string(record.gizmoOriginURL),
        gizmoOriginAuthor: coerce.string(record.gizmoOriginAuthor),
        gizmoName: coerce.string(record.gizmoName),
        gizmoDescription: coerce.string(record.gizmoDescription),
        createdAt: coerce.number(record.createdAt, {required: true}),
        receivedAt: Date.now()
      })
    }
    // TCW -- END
  })
  await db.open()

  if (userArchive) {
    // index the main user
    await db.addArchive(userArchive, {prepare: true})

    // index the followers
    db.profile.get(userArchive).then(async profile => {
      profile.followUrls.forEach(url => db.addArchive(url))
    })
  }

  return {
    db,

    async close ({destroy} = {}) {
      if (db) {
        var name = db.name
        await db.close()
        if (destroy) {
          await InjestDB.delete(name)
        }
        this.db = null
      }
    },

    addArchive (a) { return db.addArchive(a, {prepare: true}) },
    addArchives (as) { return db.addArchives(as, {prepare: true}) },
    removeArchive (a) { return db.removeArchive(a) },
    listArchives () { return db.listArchives() },

    async pruneUnfollowedArchives (userArchive) {
      var profile = await db.profile.get(userArchive)
      var archives = db.listArchives()
      await Promise.all(archives.map(a => {
        if (profile.followUrls.indexOf(a.url) === -1) {
          return db.removeArchive(a)
        }
      }))
    },

    // profiles api
    // =

    getProfile (archive) {
      var archiveUrl = coerce.archiveUrl(archive)
      return db.profile.get(archiveUrl)
    },

    setProfile (archive, profile) {
      var archiveUrl = coerce.archiveUrl(archive)
      return db.profile.upsert(archiveUrl, profile)
    },

    async setAvatar (archive, imgData, extension) {
      archive = coerce.archive(archive)
      const filename = `avatar.${extension}`

      if (archive) {
        await archive.writeFile(filename, imgData)
        await archive.commit()
      }
      return db.profile.upsert(archive, {avatar: filename})
    },

    async follow (archive, target, name) {
      // update the follow record
      var archiveUrl = coerce.archiveUrl(archive)
      var targetUrl = coerce.archiveUrl(target)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.follows = record.follows || []
        if (!record.follows.find(f => f.url === targetUrl)) {
          record.follows.push({url: targetUrl, name})
        }
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to follow: no profile record exists. Run setProfile() before follow().')
      }
      // index the target
      await db.addArchive(target)
    },

    async unfollow (archive, target) {
      // update the follow record
      var archiveUrl = coerce.archiveUrl(archive)
      var targetUrl = coerce.archiveUrl(target)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.follows = record.follows || []
        record.follows = record.follows.filter(f => f.url !== targetUrl)
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to unfollow: no profile record exists. Run setProfile() before unfollow().')
      }
      // unindex the target
      await db.removeArchive(target)
    },

    getFollowersQuery (archive) {
      var archiveUrl = coerce.archiveUrl(archive)
      return db.profile.where('followUrls').equals(archiveUrl)
    },

    listFollowers (archive) {
      return this.getFollowersQuery(archive).toArray()
    },

    countFollowers (archive) {
      return this.getFollowersQuery(archive).count()
    },

    async isFollowing (archiveA, archiveB) {
      var archiveBUrl = coerce.archiveUrl(archiveB)
      var profileA = await db.profile.get(archiveA)
      return profileA.followUrls.indexOf(archiveBUrl) !== -1
    },

    async listFriends (archive) {
      var followers = await this.listFollowers(archive)
      await Promise.all(followers.map(async follower => {
        follower.isFriend = await this.isFollowing(archive, follower.url)
      }))
      return followers.filter(f => f.isFriend)
    },

    async countFriends (archive) {
      var friends = await this.listFriends(archive)
      return friends.length
    },

    async isFriendsWith (archiveA, archiveB) {
      var [a, b] = await Promise.all([
        this.isFollowing(archiveA, archiveB),
        this.isFollowing(archiveB, archiveA)
      ])
      return a && b
    },

    // broadcasts api
    // =

    broadcast (archive, {text, threadRoot, threadParent}) {
      text = coerce.string(text)
      threadParent = threadParent ? coerce.recordUrl(threadParent) : undefined
      threadRoot = threadRoot ? coerce.recordUrl(threadRoot) : threadParent
      if (!text) throw new Error('Must provide text')
      const createdAt = Date.now()
      return db.broadcasts.add(archive, {text, threadRoot, threadParent, createdAt})
    },

    getBroadcastsQuery ({author, after, before, offset, limit, reverse} = {}) {
      var query = db.broadcasts
      if (author) {
        author = coerce.archiveUrl(author)
        after = after || 0
        before = before || Infinity
        query = query.where('_origin+createdAt').between([author, after], [author, before])
      } else if (after || before) {
        after = after || 0
        before = before || Infinity
        query = query.where('createdAt').between(after, before)
      } else {
        query = query.orderBy('createdAt')
      }
      if (offset) query = query.offset(offset)
      if (limit) query = query.limit(limit)
      if (reverse) query = query.reverse()
      return query
    },

    getRepliesQuery (threadRootUrl, {offset, limit, reverse} = {}) {
      var query = db.broadcasts.where('threadRoot').equals(threadRootUrl)
      if (offset) query = query.offset(offset)
      if (limit) query = query.limit(limit)
      if (reverse) query = query.reverse()
      return query
    },

    async listBroadcasts (opts = {}, query) {
      var promises = []
      query = query || this.getBroadcastsQuery(opts)
      var broadcasts = await query.toArray()

      // fetch author profile
      if (opts.fetchAuthor) {
        let profiles = {}
        promises = promises.concat(broadcasts.map(async b => {
          if (!profiles[b._origin]) {
            profiles[b._origin] = this.getProfile(b._origin)
          }
          b.author = await profiles[b._origin]
        }))
      }

      // tabulate votes
      if (opts.countVotes) {
        promises = promises.concat(broadcasts.map(async b => {
          b.votes = await this.countVotes(b._url)
        }))
      }

      // fetch replies
      if (opts.fetchReplies) {
        promises = promises.concat(broadcasts.map(async b => {
          b.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(b._url))
        }))
      }

      await Promise.all(promises)
      return broadcasts
    },

    countBroadcasts (opts, query) {
      query = query || this.getBroadcastsQuery(opts)
      return query.count()
    },

    async getBroadcast (record) {
      const recordUrl = coerce.recordUrl(record)
      record = await db.broadcasts.get(recordUrl)
      record.author = await this.getProfile(record._origin)
      record.votes = await this.countVotes(recordUrl)
      record.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(recordUrl))
      return record
    },

    // votes api
    // =

    vote (archive, {vote, subject}) {
      vote = coerce.vote(vote)
      if (!subject) throw new Error('Subject is required')
      if (subject._url) subject = subject._url
      if (subject.url) subject = subject.url
      subject = coerce.datUrl(subject)
      const createdAt = Date.now()
      return db.votes.add(archive, {vote, subject, createdAt})
    },

    getVotesQuery (subject) {
      return db.votes.where('subject').equals(coerce.voteSubject(subject))
    },

    listVotes (subject) {
      return this.getVotesQuery(subject).toArray()
    },

    async countVotes (subject) {
      var res = {up: 0, down: 0, value: 0, upVoters: [], currentUsersVote: 0}
      await this.getVotesQuery(subject).each(record => {
        res.value += record.vote
        if (record.vote === 1) {
          res.upVoters.push(record._origin)
          res.up++
        }
        if (record.vote === -1) {
          res.down--
        }
        if (userArchive && record._origin === userArchive.url) {
          res.currentUsersVote = record.vote
        }
      })
      return res
    },

    // TCW -- gizmo api

    async gizmo (archive, {
      gizmoOriginURL,
      gizmoOriginArchive,
      gizmoOriginAuthor,
      gizmoName,
      gizmoDescription,
      gizmoDocs,
      gizmoJS
    }) {
      gizmoOriginURL = coerce.string(gizmoOriginURL)
      gizmoOriginArchive = coerce.string(gizmoOriginArchive)
      gizmoOriginAuthor = coerce.string(gizmoOriginAuthor)
      gizmoName = coerce.string(gizmoName)
      gizmoDescription = coerce.string(gizmoDescription)
      gizmoDocs = coerce.string(gizmoDocs)
      gizmoJS = coerce.string(gizmoJS)
      const createdAt = Date.now()
      if (gizmoOriginURL) {
        return db.gizmos.add(archive, {
          gizmoOriginURL,
          gizmoOriginArchive,
          gizmoOriginAuthor,
          gizmoName,
          gizmoDescription,
          gizmoDocs,
          gizmoJS,
          createdAt
        })
      } else {
        gizmoOriginURL = await db.gizmos.add(archive, {
          gizmoOriginURL,
          gizmoOriginArchive,
          gizmoOriginAuthor,
          gizmoName,
          gizmoDescription,
          gizmoDocs,
          gizmoJS,
          createdAt
        })
        this.updateGizmoOrigin(gizmoOriginURL)
      }
    },

    async updateGizmoOrigin (gizmoOriginURL) {
      await db.gizmos.update(gizmoOriginURL, {gizmoOriginURL})
    },

    async subscribeToGizmo (archive, gizmo) {
      var archiveUrl = coerce.archiveUrl(archive)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.gizmoURLs = record.gizmoURLs || []
        if (!record.gizmoURLs.find(g => g.gizmoOriginURL === gizmo.gizmoOriginURL)) {
          record.gizmoURLs.push(gizmo.gizmoOriginURL)
        }
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to subscribe: no profile record exists. Run setProfile() before subscribeToGizmo().')
      }
    },

    async unsubscribeFromGizmo (archive, gizmo) {
      var archiveUrl = coerce.archiveUrl(archive)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.gizmoURLs = record.gizmoURLs || []
        record.gizmoURLs = record.gizmoURLs.filter(g => g !== gizmo.gizmoOriginURL)
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to unsubscribe: no profile record exists. Run setProfile() before unsubscribeFromGizmo().')
      }
    },

    async isSubscribed (archive, gizmo) {
      var gizmoURL = coerce.recordUrl(gizmo.gizmoOriginURL)
      var profile = await db.profile.get(archive)
      return profile.gizmoURLs.indexOf(gizmoURL) !== -1
    },

    async removeGizmo (archive, gizmo) {
      console.log('gizmo in remove', gizmo)
      const changes = await db.gizmos.where('_origin+gizmoOriginURL').equals([archive, gizmo.gizmoOriginURL]).delete()
      if (changes === 0) {
        throw new Error('Failed to delete: no gizmo record exists.')
      }
      await db.removeArchive(gizmo._url)
    },

    getGizmosQuery ({author, after, before, offset, limit, reverse} = {}) {
      var query = db.gizmos
      if (author) {
        author = coerce.archiveUrl(author)
        after = after || 0
        before = before || Infinity
        query = query.where('_origin+createdAt').between([author, after], [author, before])
      } else if (after || before) {
        after = after || 0
        before = before || Infinity
        query = query.where('createdAt').between(after, before)
      } else {
        query = query.orderBy('createdAt')
      }
      if (offset) query = query.offset(offset)
      if (limit) query = query.limit(limit)
      if (reverse) query = query.reverse()
      return query
    },

    async listGizmos (opts = {}, query) {
      var promises = []
      query = query || this.getGizmosQuery(opts)
      var gizmos = await query.toArray()

      // fetch author profile
      if (opts.fetchAuthor) {
        let profiles = {}
        promises = promises.concat(gizmos.map(async b => {
          if (!profiles[b._origin]) {
            profiles[b._origin] = this.getProfile(b._origin)
          }
          b.author = await profiles[b._origin]
        }))
      }

      // tabulate votes
      if (opts.countVotes) {
        promises = promises.concat(gizmos.map(async b => {
          b.votes = await this.countVotes(b._url)
        }))
      }

      await Promise.all(promises)
      return gizmos
    },

    countGizmos (opts, query) {
      query = query || this.getGizmosQuery(opts)
      return query.count()
    },

    async getGizmo (record) {
      const recordUrl = coerce.recordUrl(record)
      record = await db.gizmos.get(recordUrl)
      record.author = await this.getProfile(record._origin)
      record.votes = await this.countVotes(recordUrl)
      return record
    },

    // TCW -- postscripts api

    postscript (archive, {
      postscriptJS,
      postscriptHTTP,
      postscriptInfo,
      gizmoOriginArchive,
      gizmoOriginURL,
      gizmoOriginAuthor,
      gizmoName,
      gizmoDescription
    }) {
      postscriptJS = coerce.string(postscriptJS)
      postscriptHTTP = coerce.string(postscriptHTTP)
      postscriptInfo = coerce.string(postscriptInfo)
      gizmoOriginArchive = coerce.string(gizmoOriginArchive)
      gizmoOriginURL = coerce.string(gizmoOriginURL)
      gizmoOriginAuthor = coerce.string(gizmoOriginAuthor)
      gizmoName = coerce.string(gizmoName)
      gizmoDescription = coerce.string(gizmoDescription)
      const createdAt = Date.now()

      return db.postscripts.add(archive, {
        postscriptJS,
        postscriptHTTP,
        postscriptInfo,
        gizmoOriginArchive,
        gizmoOriginURL,
        gizmoOriginAuthor,
        gizmoName,
        gizmoDescription,
        createdAt
      })
    },

    getPostscriptsQuery ({author, after, before, offset, limit, reverse} = {}) {
      var query = db.postscripts
      if (author) {
        author = coerce.archiveUrl(author)
        after = after || 0
        before = before || Infinity
        query = query.where('_origin+createdAt').between([author, after], [author, before])
      } else if (after || before) {
        after = after || 0
        before = before || Infinity
        query = query.where('createdAt').between(after, before)
      } else {
        query = query.orderBy('createdAt')
      }
      if (offset) query = query.offset(offset)
      if (limit) query = query.limit(limit)
      if (reverse) query = query.reverse()
      return query
    },

    async listPostscripts (opts = {}, query) {
      var promises = []
      query = query || this.getPostscriptsQuery(opts)
      var postscripts = await query.toArray()

      // fetch author profile
      if (opts.fetchAuthor) {
        let profiles = {}
        promises = promises.concat(postscripts.map(async b => {
          if (!profiles[b._origin]) {
            profiles[b._origin] = this.getProfile(b._origin)
          }
          b.author = await profiles[b._origin]
        }))
      }

      // tabulate votes
      if (opts.countVotes) {
        promises = promises.concat(postscripts.map(async b => {
          b.votes = await this.countVotes(b._url)
        }))
      }

      await Promise.all(promises)
      return postscripts
    },

    countPostscripts (opts, query) {
      query = query || this.getPostscriptsQuery(opts)
      return query.count()
    },

    async getPostscript (record) {
      const recordUrl = coerce.recordUrl(record)
      record = await db.postscripts.get(recordUrl)
      record.author = await this.getProfile(record._origin)
      record.votes = await this.countVotes(recordUrl)
      return record
    }
  }
}
