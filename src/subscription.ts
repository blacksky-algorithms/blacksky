import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import {
  OutputSchema as ThreadPost
} from './lexicon/types/app/bsky/feed/getPostThread'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { AtpAgent, BlobRef } from '@atproto/api'
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

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    dotenv.config()

    const handle = process.env.IDENTIFIER ?? ''
    const password = process.env.PASSWORD ?? ''
    const uri = process.env.BLACKSKYTHREAD ?? ''

    const agent = new AtpAgent({ service: 'https://bsky.social' })
    await agent.login({ identifier: handle, password })

    const blackskyThread = await agent.api.app.bsky.feed.getPostThread({uri})
    const blacksky = new Set(parseReplies(blackskyThread.data.thread))

    // TO DO: Maybe replace with an API
    const hardcoded_adds = ['did:plc:j4bko7yvzthmufkoxtzcoauh','did:plc:l4g436iw6lmd7ywrqz4lko5w','did:plc:mnkuzinn3jjjytuwdlw265ql','did:plc:f7gdbr6mkxcukdnjd7vdl4q4','did:plc:f4ctxz5nwkyfedkfvyxbtpgq','did:plc:7o55wjsyg2ylsmlr5to6gb67','did:plc:xum72mip7ti5niwqbgpvaqn4','did:plc:elelkrcnhv3adswxrkqkdt5k','did:plc:x4dmyp6bfmu3mshhx3fi4ko5','did:plc:m3ysxi4vufhxrg7syo55pt6r','did:plc:hxo5ss5y5p5wrhbxujcvru7w','did:plc:h5mzskhcrcjkf7vr4gadmf2c','did:plc:hsfzdq3icftr7vdp4d7krv4t','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:yddfcdhhinii6fj6hyulnzpk','did:plc:qlafttm5vzbjna7xltdfzdxh','did:plc:yl7wcldipsfnjdww2jg5mnrv','did:plc:tvt74yrjs4z4xvo3ov2vmk7f','did:plc:fpgknm3s36mthymrcjtgyerq','did:plc:vqs225hr64cjo5ookixhdqwr','did:plc:hqp33n2fqericzhtg6zprsog','did:plc:ccjaows3pzilatcha3n3fedj','did:plc:nsy2e4vupxndly4adhszebcy','did:plc:wcmdstkjsrghcsa6e76kocbs','did:plc:w6dfrjeqdshsmb2swhgmj4ju','did:plc:bei7c4ixr4dfmxunwnldt2xk']
    const hardcoded_removes = ['did:plc:buofnbcavecxm3kr6x5npusi']
    for (let add of hardcoded_adds) {
      blacksky.add(add)
    }
    for (let rm of hardcoded_removes) {
      blacksky.delete(rm)
    }

    const ops = await getOpsByType(evt)

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // Filter for authors from the blacksky thread
        return blacksky.has(create.author)
      })
      .map((create) => {
        // Create Blacksky posts in db
        console.log(`Blacksky author ${create.author}!`)
        console.log(create)
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
