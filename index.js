const InjestDB = require('ingestdb')
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
        subgizmos: coerce.arrayOfSubgizmos(record.subgizmos)
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

    gizmos: {
      primaryKey: 'createdAt',
      index: ['createdAt', '_origin+createdAt'],
      validator: record => ({
        gizmoName: coerce.string(record.gizmoName),
        gizmoDescription: coerce.string(record.gizmoDescription),
        gizmoDocs: coerce.string(record.gizmoDocs),
        gizmoDependencies: coerce.arrayOfDependencies(record.gizmoDependencies),
        postDependencies: coerce.arrayOfDependencies(record.postDependencies),
        gizmoJS: coerce.string(record.gizmoJS),
        gizmoCSS: coerce.string(record.gizmoCSS),
        postJS: coerce.string(record.postJS),
        postCSS: coerce.string(record.postCSS),
        createdAt: coerce.number(record.createdAt, {required: true}),
        receivedAt: Date.now()
      })
    },

    posts: {
      primaryKey: 'createdAt',
      index: ['createdAt', '_origin+createdAt'],
      validator: record => ({
        postParams: coerce.string(record.postParams),
        postHTTP: coerce.string(record.postHTTP),
        postText: coerce.string(record.postText),
        gizmoURL: coerce.string(record.gizmoURL),
        createdAt: coerce.number(record.createdAt, {required: true}),
        receivedAt: Date.now()
      })
    }
  })
  await db.open()

  if (userArchive) {
    // index the main user
    await db.addArchive(userArchive, {prepare: true})

    // index the followers
    db.profile.get(userArchive).then(async profile => {
      if (profile) {
        profile.followUrls.forEach(url => db.addArchive(url))
      }
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

    async setProfile (archive, profile) {
      var archiveUrl = coerce.archiveUrl(archive)
      await db.profile.upsert(archiveUrl, profile)
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
      gizmoName,
      gizmoDescription,
      gizmoDocs,
      gizmoDependencies,
      postDependencies,
      gizmoJS,
      gizmoCSS,
      postJS,
      postCSS
    }) {
      gizmoName = coerce.string(gizmoName)
      gizmoDescription = coerce.string(gizmoDescription)
      gizmoDocs = coerce.string(gizmoDocs)
      gizmoDependencies = coerce.arrayOfDependencies(gizmoDependencies)
      gizmoDependencies = await Promise.all(gizmoDependencies.map(async d => await this.getGizmo(d.url)))
      postDependencies = coerce.arrayOfDependencies(postDependencies)
      postDependencies = await Promise.all(postDependencies.map(async d => await this.getGizmo(d.url)))
      gizmoJS = coerce.string(gizmoJS)
      gizmoCSS = coerce.string(gizmoCSS)
      postJS = coerce.string(postJS)
      postCSS = coerce.string(postCSS)
      const createdAt = Date.now()
      return db.gizmos.add(archive, {
        gizmoName,
        gizmoDescription,
        gizmoDocs,
        gizmoDependencies,
        postDependencies,
        gizmoJS,
        gizmoCSS,
        postJS,
        postCSS,
        createdAt
      })
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

      if (opts.subscriber) {
        let subscriber = await this.getProfile(opts.subscriber)
        gizmos = gizmos.filter(g => {
          return !!subscriber.subgizmos.find(sg => sg.url === g._url)
        })
      }

      if (opts.loadShop) {
        if (!opts.author) {
          throw new Error('An author must be provided when loading the Shop.')
        } else {
          let author = coerce.archiveUrl(opts.author)
          gizmos = gizmos.filter(g => {
            return g._origin === author
          })
        }
      }

      if (opts.fetchAuthor) {
        let profiles = {}
        promises = promises.concat(gizmos.map(async g => {
          if (!profiles[g._origin]) {
            profiles[g._origin] = await this.getProfile(g._origin)
          }
          g.author = await profiles[g._origin]
        }))
      }

      if (opts.fetchReplies) {
        promises = promises.concat(gizmos.map(async g => {
          g.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(g._url))
        }))
      }

      if (opts.countVotes) {
        promises = promises.concat(gizmos.map(async g => {
          g.votes = await this.countVotes(g._url)
        }))
      }

      if (opts.checkIfSubscribed) {
        promises = promises.concat(gizmos.map(async g => {
          g.isSubscribed = await this.isSubscribed(opts.checkIfSubscribed, g)
        }))
      }

      if (opts.fetchGizmoDependencies) {
        promises = promises.concat(gizmos.map(async g => {
          g.fullDependencies = await this.getGizmoDependencies(g)
        }))
      }

      await Promise.all(promises)
      return gizmos
    },

    countGizmos (opts, query) {
      query = query || this.getGizmosQuery(opts)
      return query.count()
    },

    async getGizmo (gizmo, opts = {}) {
      const gizmoURL = coerce.recordUrl(gizmo)
      gizmo = await db.gizmos.get(gizmoURL)
      if (opts.fetchAuthor) {
        gizmo.author = await this.getProfile(gizmo._origin)
      }
      if (opts.countVotes) {
        gizmo.votes = await this.countVotes(gizmoURL)
      }
      if (opts.fetchReplies) {
        gizmo.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(gizmoURL))
      }
      if (opts.checkIfSubscribed) {
        if (!opts.requester) {
          throw new Error('The archive of the requester must be provided when checking if subscribed.')
        } else {
          const requesterURL = coerce.archiveUrl(opts.requester)
          gizmo.isSubscribed = await this.isSubscribed(requesterURL, gizmo)
        }
      }
      if (opts.fetchAllDependencies) {
        gizmo = await this.getAllDependencies(gizmo)
      }
      return gizmo
    },

    // !! -- need to refactor -- !!

    async getDependency (gizmo) {
      const gizmoURL = coerce.recordUrl(gizmo)
      const dependency = await db.gizmos.get(gizmoURL)
      return dependency
    },

    async getAllDependencies (gizmo) {
      if (gizmo.gizmoDependencies.length === 0) {
        return gizmo
      }
      let dependencies = await gizmo.gizmoDependencies.map(d => this.getDependency(d))
      await Promise.all(dependencies)
      let childDependencies = {}
      await Promise.all(dependencies.map(async (d, idx) => {
        childDependencies[idx] = await this.getAllDependencies(d)
      }))
      gizmo.childDependencies = childDependencies
      return gizmo
    },

    async getPostDependencies (gizmo) {
      let postDependencies = []
      postDependencies = await Promise.all(gizmo.postDependencies.map(async d => await this.getGizmo(d.url)))
      return postDependencies
    },

    async getGizmoDependencies (gizmo) {
      let fullDependencies = []
      fullDependencies = await Promise.all(gizmo.gizmoDependencies.map(async d => await this.getGizmo(d.url)))
      return fullDependencies
    },

    // !! -- need to refactor -- !!

    async subscribe (archive, gizmo) {
      var archiveUrl = coerce.archiveUrl(archive)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.subgizmos = record.subgizmos || []
        if (!record.subgizmos.find(sg => sg.url === gizmo._url)) {
          record.subgizmos.push({
            url: gizmo._url,
            origin: gizmo._origin,
            author: gizmo.author.name,
            name: gizmo.gizmoName
          })
        }
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to subscribe: gizmo record already exists.')
      }
    },

    async subscribeMany (archive, gizmoArray) {
      var archiveUrl = coerce.archiveUrl(archive)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.subgizmos = record.subgizmos || []
        gizmoArray.forEach(gizmo => {
          if (!record.subgizmos.find(sg => sg.url === gizmo._url)) {
            record.subgizmos.push({
              url: gizmo._url,
              origin: gizmo._origin,
              author: gizmo.author.name,
              name: gizmo.gizmoName
            })
          }
        })
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to subscribe: gizmo record already exists.')
      }
    },

    async unsubscribe (archive, gizmo) {
      var archiveUrl = coerce.archiveUrl(archive)
      var changes = await db.profile.where('_origin').equals(archiveUrl).update(record => {
        record.subgizmos = record.subgizmos || []
        record.subgizmos = record.subgizmos.filter(sg => sg.url !== gizmo._url)
        return record
      })
      if (changes === 0) {
        throw new Error('Failed to unsubscribe: no gizmo record exists.')
      }
    },

    async isSubscribed (archive, gizmo) {
      var archiveURL = coerce.archiveUrl(archive)
      var gizmoURL = coerce.recordUrl(gizmo._url)
      var profile = await db.profile.get(archiveURL)
      return !!profile.subgizmos.find(sg => sg.url === gizmoURL)
    },

    // TCW -- posts api

    post (archive, {
      postParams,
      postHTTP,
      postText,
      gizmoURL
    }) {
      postParams = coerce.string(postParams)
      postHTTP = coerce.string(postHTTP)
      postText = coerce.string(postText)
      gizmoURL = coerce.string(gizmoURL)
      const createdAt = Date.now()

      return db.posts.add(archive, {
        postParams,
        postHTTP,
        postText,
        gizmoURL,
        createdAt
      })
    },

    getPostsQuery ({author, after, before, offset, limit, reverse} = {}) {
      var query = db.posts
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

    async listPosts (opts = {}, query) {
      var promises = []
      query = query || this.getPostsQuery(opts)
      var posts = await query.toArray()

      if (opts.currentURL) {
        posts = posts.filter(p => {
          return p.postHTTP === opts.currentURL
        })
      }

      // fetch author profile
      if (opts.fetchAuthor) {
        let profiles = {}
        promises = promises.concat(posts.map(async p => {
          if (!profiles[p._origin]) {
            profiles[p._origin] = await this.getProfile(p._origin)
          }
          p.author = await profiles[p._origin]
        }))
      }

      if (opts.fetchGizmo) {
        promises = promises.concat(posts.map(async p => {
          p.gizmo = await this.getGizmo(p.gizmoURL, {
            fetchAuthor: true,
            fetchReplies: true,
            countVotes: true,
            checkIfSubscribed: true,
            requester: opts.requester
          })
        }))
      }

      // tabulate votes
      if (opts.countVotes) {
        promises = promises.concat(posts.map(async p => {
          p.votes = await this.countVotes(p._url)
        }))
      }

      if (opts.fetchReplies) {
        promises = promises.concat(posts.map(async p => {
          p.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(p._url))
        }))
      }

      await Promise.all(promises)

      promises = []
      if (opts.fetchPostDependencies) {
        promises = promises.concat(posts.map(async p => {
          p.postDependencies = await this.getPostDependencies(p.gizmo)
        }))
      }

      await Promise.all(promises)

      return posts
    },

    countPosts (opts, query) {
      query = query || this.getPostsQuery(opts)
      return query.count()
    },

    async getPost (requester, post) {
      const requesterUrl = coerce.archiveUrl(requester)
      const postUrl = coerce.recordUrl(post)
      post = await db.posts.get(postUrl)
      const gizmoURL = coerce.recordUrl(post.gizmoURL)
      post.author = await this.getProfile(post._origin)
      post.votes = await this.countVotes(postUrl)
      post.gizmo = await this.getGizmo(gizmoURL, {
        fetchAuthor: true,
        fetchReplies: true,
        countVotes: true,
        checkIfSubscribed: true,
        requester: requesterUrl
      })
      post.replies = await this.listBroadcasts({fetchAuthor: true}, this.getRepliesQuery(postUrl))
      return post
    }
  }
}
