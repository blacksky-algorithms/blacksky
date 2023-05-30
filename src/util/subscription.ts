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
    const hardcoded_adds = ['did:plc:j4bko7yvzthmufkoxtzcoauh','did:plc:l4g436iw6lmd7ywrqz4lko5w','did:plc:mnkuzinn3jjjytuwdlw265ql','did:plc:f7gdbr6mkxcukdnjd7vdl4q4','did:plc:f4ctxz5nwkyfedkfvyxbtpgq','did:plc:7o55wjsyg2ylsmlr5to6gb67','did:plc:xum72mip7ti5niwqbgpvaqn4','did:plc:elelkrcnhv3adswxrkqkdt5k','did:plc:x4dmyp6bfmu3mshhx3fi4ko5','did:plc:m3ysxi4vufhxrg7syo55pt6r','did:plc:hxo5ss5y5p5wrhbxujcvru7w','did:plc:h5mzskhcrcjkf7vr4gadmf2c','did:plc:hsfzdq3icftr7vdp4d7krv4t','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:yddfcdhhinii6fj6hyulnzpk','did:plc:qlafttm5vzbjna7xltdfzdxh','did:plc:yl7wcldipsfnjdww2jg5mnrv','did:plc:tvt74yrjs4z4xvo3ov2vmk7f','did:plc:fpgknm3s36mthymrcjtgyerq','did:plc:vqs225hr64cjo5ookixhdqwr','did:plc:hqp33n2fqericzhtg6zprsog','did:plc:ccjaows3pzilatcha3n3fedj','did:plc:nsy2e4vupxndly4adhszebcy','did:plc:wcmdstkjsrghcsa6e76kocbs','did:plc:w6dfrjeqdshsmb2swhgmj4ju','did:plc:bei7c4ixr4dfmxunwnldt2xk','did:plc:4e3wxro75sds7cc2yfgnbnrc','did:plc:72g57t2jyqvy52w273mvfldl','did:plc:kpssseyzxoreqmqbh6sxbchm','did:plc:mvmfhonrfi2pwippivfcrvdw','did:plc:bkf2cmel54nxephykahosik2','did:plc:ahjc3qy354o4sqm2ynzw2aoe','did:plc:3bx2dysw3d3p54j4kzeridub','did:plc:fdgcujcsqzgxddcovzat23vb','did:plc:w4rlevqrhzxkc53tet2x6cs6','did:plc:wonrxml4kfrz7gnok6kciy6j','did:plc:nb6cbokqbfuq3gn4ifswjwam','did:plc:nzd4yp6wlmf3q5izext7vl5x','did:plc:h55dbmzzypr2hrxrjcjjcrq3','did:plc:vo7on74ip5lnlwtbm4aobim2','did:plc:fuknyynznmk26wg35ydjwcon','did:plc:ldpih6ykqs2wacnn3jug4ayf','did:plc:v2lyrftkaiu4yhy4ld4nt7qa','did:plc:avpi2kz2piugcemn7hdxx6h7','did:plc:5gt4sggyf3dhi5ee4lwebspz','did:plc:45k4ag25qqmksteyuo67yxpl','did:plc:5ypnozmulyjkgzpffkxwzbgn','did:plc:ieznolj4yeg7ezgcetdpqdy4','did:plc:57vlzz2egy6eqr4nksacmbht','did:plc:c4npsvzgb5yaeiu3ujg5nffy','did:plc:z45bsilgbevbobfnytjxcoll','did:plc:nyy6xonlicu4fdktafg3dv7s','did:plc:fcbcd54ypn2hkmfvyv2n5hae','did:plc:7gdi4pckwbqgnb3soazmhwrw','did:plc:lkg2uki5kkvjhyiehgai43li','did:plc:vtfgoubo76nku4q3wsx3iyw7','did:plc:k6mhxj4jl4hol635stqhkwib','did:plc:c6sulb24hiwth6nxdljsekao','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:4dcp6k3kcop4jjjgssje5epl','did:plc:trhuguen3cmupmuai5lwy6fi','did:plc:bppz56h6f67t2tclkgq3h5oe','did:plc:haxycql755s4a7jl4hvmbfj3','did:plc:dh74sr7cc6s2psd4nacdzbpc','did:plc:e2ctbutx6kya6si4if5ngjmm','did:plc:yxepj65xiqw36ym547ikefba','did:plc:hy4mxyxvprjwltgj7o3dq47g','did:plc:rqknamd5zzwfekpqqsnylzqr','did:plc:whksjy5y7shgddk6o6qi3bff','did:plc:j5wdt346dbekcpjszpu4tvth','did:plc:3sczr6k6642eslsiervj272y','did:plc:xefaxesvgy7smyivbkvyojog','did:plc:p73662ybe643hnkpi2uyb4cl','did:plc:y742yf6jzv7rznyxsyn3nyo6','did:plc:lyylolptc5uxturmnetnhqfs','did:plc:5odb5x72xa7nltkjtf2gch2g','did:plc:75d33vgka7l7sc7zinztmgvn','did:plc:ekdqk6e3c5lkw3iwiigcy2gu','did:plc:kosvugygi3injgy4wwjtjsbs','did:plc:n2ylkhqdzjykr3mwwdpmhcsd','did:plc:6b7qj4tbzobgoduddyecfr22','did:plc:slo6mpsu7oytu6i5qt5cfeoo','did:plc:ciqqpyfnd2brriuxtjuv57xh','did:plc:p2ahkyueh77xh3p3fvyhabi7','did:plc:wjtbed3i2r7zki5a2bpuhurb','did:plc:2tjiu3ydbuaznxooclmxjetd','did:plc:yeoi4if4g6xvt7ycwlql2zjg','did:plc:tylusml32njglaw2lgiha35q','did:plc:wtonzz3kejo5dbrnshen5und','did:plc:oq5bzh5duusvc2san2apicvx','did:plc:djpchaxpyfqoc7ezu7xsmdj6','did:plc:iqx4itcsb2pwrtewjucxrnjl','did:plc:c54rnpmsf62o3wmqfqhwk3o2','did:plc:wven6ugrpgsdrxjuea3obkll','did:plc:ekcyikfp2mpymgtyzz7wkfuj','did:plc:zvyn4tctscn44fj4nrobupq5','did:plc:2vbrsdk6xr3iwerdkpvtsszv','did:plc:nvz6dcn63ftrzifnontes37m','did:plc:6u6uwdppytvyhgjvgucpjnig','did:plc:cewopa6cy2zuz742o4vmuvct','did:plc:ahptkqxcvmzzbblabrmfhi5v','did:plc:76il4ns5xbqbsqjwibjgcnsk','did:plc:5rkhafcde4xm4h5vpmo7ny74','did:plc:mglaxt4f53shpsu4u7nkmpyv','did:plc:3l6xjxyridfa7bj3wsramugy','did:plc:5lft52hjdtrvph4gcv6oyrfh','did:plc:qful3gp56v3r3p5byb3rmifs','did:plc:7mjbn64fshkrr6fr5uzeqzrs','did:plc:fhrogbenudj3j2cnpu5jfz2c','did:plc:kxbxcnefzqrxel3x2gmmkxza','did:plc:j2fkbwzibk6zopkpyoagzh62','did:plc:fvfbdqq3omplsfvzodbt6b5t','did:plc:bacph727xbk55zfu4tbxvzjp','did:plc:l5eyuhw7sx5nt42nfuzw45fy','did:plc:mukqkfmxqhfjklk3mkd2jbb3','did:plc:yxus52nvtcqpkyimdxmbry7u','did:plc:j2f2bfwdagwtyyvepo7ls34s','did:plc:pzqzwfhobi4ezaldfcnwe5wq']
    const hardcoded_removes = ['did:plc:buofnbcavecxm3kr6x5npusi','did:plc:le6rojzdlcqqbun4mwlo64wr']
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
