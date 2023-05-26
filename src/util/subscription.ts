import { Subscription } from '@atproto/xrpc-server'
import { cborToLexRecord, readCar } from '@atproto/repo'
import { BlobRef } from '@atproto/lexicon'
import { ids, lexicons } from '../lexicon/lexicons'
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Record as RepostRecord } from '../lexicon/types/app/bsky/feed/repost'
import { Record as LikeRecord } from '../lexicon/types/app/bsky/feed/like'
import { Record as FollowRecord } from '../lexicon/types/app/bsky/graph/follow'
import {
  Commit,
  OutputSchema as RepoEvent,
  isCommit,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import {
  OutputSchema as ThreadPost
} from '../lexicon/types/app/bsky/feed/getPostThread'
import { Database } from '../db'
import { AtpAgent } from '@atproto/api'
import dotenv from 'dotenv'

function parseReplies(threadPost) {
  let identifiers: any[] = [];
  if (threadPost.replies) {
    identifiers = [...new Set(threadPost.replies
      .map((reply) => {
        return parseReplies(reply)
      }))]
  }
  identifiers.push(threadPost.post.author.did)
  return identifiers.flat()
}

export abstract class FirehoseSubscriptionBase {
  public sub: Subscription<RepoEvent>

  constructor(public db: Database, public service: string) {
    this.sub = new Subscription({
      service: service,
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: () => this.getCursor(),
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(
            ids.ComAtprotoSyncSubscribeRepos,
            value,
          )
        } catch (err) {
          console.error('repo subscription skipped invalid message', err)
        }
      },
    })
  }

  abstract handleEvent(evt: RepoEvent, blacksky): Promise<void>

  async run(subscriptionReconnectDelay: number) {
    try {
      dotenv.config()

    const handle = process.env.IDENTIFIER ?? ''
    const password = process.env.PASSWORD ?? ''
    const uri = process.env.BLACKSKYTHREAD ?? ''

    const agent = new AtpAgent({ service: 'https://bsky.social' })
    await agent.login({ identifier: handle, password })

    const blackskyThread = await agent.api.app.bsky.feed.getPostThread({uri})
    const blacksky = new Set(parseReplies(blackskyThread.data.thread))

    // TO DO: Maybe replace with an API
    const hardcoded_adds = ['did:plc:j4bko7yvzthmufkoxtzcoauh','did:plc:l4g436iw6lmd7ywrqz4lko5w','did:plc:mnkuzinn3jjjytuwdlw265ql','did:plc:f7gdbr6mkxcukdnjd7vdl4q4','did:plc:f4ctxz5nwkyfedkfvyxbtpgq','did:plc:7o55wjsyg2ylsmlr5to6gb67','did:plc:xum72mip7ti5niwqbgpvaqn4','did:plc:elelkrcnhv3adswxrkqkdt5k','did:plc:x4dmyp6bfmu3mshhx3fi4ko5','did:plc:m3ysxi4vufhxrg7syo55pt6r','did:plc:hxo5ss5y5p5wrhbxujcvru7w','did:plc:h5mzskhcrcjkf7vr4gadmf2c','did:plc:hsfzdq3icftr7vdp4d7krv4t','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:yddfcdhhinii6fj6hyulnzpk','did:plc:qlafttm5vzbjna7xltdfzdxh','did:plc:yl7wcldipsfnjdww2jg5mnrv','did:plc:tvt74yrjs4z4xvo3ov2vmk7f','did:plc:fpgknm3s36mthymrcjtgyerq','did:plc:vqs225hr64cjo5ookixhdqwr','did:plc:hqp33n2fqericzhtg6zprsog','did:plc:ccjaows3pzilatcha3n3fedj','did:plc:nsy2e4vupxndly4adhszebcy','did:plc:wcmdstkjsrghcsa6e76kocbs','did:plc:w6dfrjeqdshsmb2swhgmj4ju','did:plc:bei7c4ixr4dfmxunwnldt2xk','did:plc:4e3wxro75sds7cc2yfgnbnrc','did:plc:72g57t2jyqvy52w273mvfldl','did:plc:kpssseyzxoreqmqbh6sxbchm','did:plc:mvmfhonrfi2pwippivfcrvdw','did:plc:bkf2cmel54nxephykahosik2','did:plc:ahjc3qy354o4sqm2ynzw2aoe','did:plc:3bx2dysw3d3p54j4kzeridub']
    const hardcoded_removes = ['did:plc:buofnbcavecxm3kr6x5npusi']
    for (let add of hardcoded_adds) {
      blacksky.add(add)
    }
    for (let rm of hardcoded_removes) {
      blacksky.delete(rm)
    }

    for await (const evt of this.sub) {
      try {
        await this.handleEvent(evt, blacksky)
        } catch (err) {
          console.error('repo subscription could not handle message', err)
        }
        // update stored cursor every 20 events or so
        if (isCommit(evt) && evt.seq % 20 === 0) {
          await this.updateCursor(evt.seq)
        }
      }
    } catch (err) {
      console.error('repo subscription errored', err)
      setTimeout(() => this.run(subscriptionReconnectDelay), subscriptionReconnectDelay)
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .updateTable('sub_state')
      .set({ cursor })
      .where('service', '=', this.service)
      .execute()
  }

  async getCursor(): Promise<{ cursor?: number }> {
    const res = await this.db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', this.service)
      .executeTakeFirst()
    return res ? { cursor: res.cursor } : {}
  }
}

export const getOpsByType = async (evt: Commit): Promise<OperationsByType> => {
  const car = await readCar(evt.blocks)
  const opsByType: OperationsByType = {
    posts: { creates: [], deletes: [] },
    reposts: { creates: [], deletes: [] },
    likes: { creates: [], deletes: [] },
    follows: { creates: [], deletes: [] },
  }

  for (const op of evt.ops) {
    const uri = `at://${evt.repo}/${op.path}`
    const [collection] = op.path.split('/')

    if (op.action === 'update') continue // updates not supported yet

    if (op.action === 'create') {
      if (!op.cid) continue
      const recordBytes = car.blocks.get(op.cid)
      if (!recordBytes) continue
      const record = cborToLexRecord(recordBytes)
      const create = { uri, cid: op.cid.toString(), author: evt.repo }
      if (collection === ids.AppBskyFeedPost && isPost(record)) {
        opsByType.posts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
        opsByType.reposts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
        opsByType.likes.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyGraphFollow && isFollow(record)) {
        opsByType.follows.creates.push({ record, ...create })
      }
    }

    if (op.action === 'delete') {
      if (collection === ids.AppBskyFeedPost) {
        opsByType.posts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedRepost) {
        opsByType.reposts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedLike) {
        opsByType.likes.deletes.push({ uri })
      } else if (collection === ids.AppBskyGraphFollow) {
        opsByType.follows.deletes.push({ uri })
      }
    }
  }

  return opsByType
}

type OperationsByType = {
  posts: Operations<PostRecord>
  reposts: Operations<RepostRecord>
  likes: Operations<LikeRecord>
  follows: Operations<FollowRecord>
}

type Operations<T = Record<string, unknown>> = {
  creates: CreateOp<T>[]
  deletes: DeleteOp[]
}

type CreateOp<T> = {
  uri: string
  cid: string
  author: string
  record: T
}

type DeleteOp = {
  uri: string
}

export const isPost = (obj: unknown): obj is PostRecord => {
  return isType(obj, ids.AppBskyFeedPost)
}

export const isRepost = (obj: unknown): obj is RepostRecord => {
  return isType(obj, ids.AppBskyFeedRepost)
}

export const isLike = (obj: unknown): obj is LikeRecord => {
  return isType(obj, ids.AppBskyFeedLike)
}

export const isFollow = (obj: unknown): obj is FollowRecord => {
  return isType(obj, ids.AppBskyGraphFollow)
}

const isType = (obj: unknown, nsid: string) => {
  try {
    lexicons.assertValidRecord(nsid, fixBlobRefs(obj))
    return true
  } catch (err) {
    return false
  }
}

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
  if (Array.isArray(obj)) {
    return obj.map(fixBlobRefs)
  }
  if (obj && typeof obj === 'object') {
    if (obj.constructor.name === 'BlobRef') {
      const blob = obj as BlobRef
      return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original)
    }
    return Object.entries(obj).reduce((acc, [key, val]) => {
      return Object.assign(acc, { [key]: fixBlobRefs(val) })
    }, {} as Record<string, unknown>)
  }
  return obj
}
