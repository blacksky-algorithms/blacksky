import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent, blacksky) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)
    const hellthreadRoots = new Set<string>(['bafyreigxvsmbhdenvzaklcfnovbsjc542cu5pjmpqyyc64mdtqwsyimlvi'])

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // Filter out any posts from Hellthread
        let reply;
        if (create?.record?.hasOwnProperty('reply')) {
          ({record: {reply}} = create);
        }
        let isHellthread = hellthreadRoots.has(reply?.root?.cid)

        // Filter for authors from the blacksky thread
        let isBlackSkyAuthor = blacksky.has(create.author)

        // Filter for posts that include the #blacksky hashtag
        let hashtags: any[] = []
        create?.record?.text?.toLowerCase()
          ?.match(/#[^\s#\.\;]*/gmi)
          ?.map((hashtag) => {
            hashtags.push(hashtag)
          })

        return (isBlackSkyAuthor || hashtags.includes('#blacksky') || hashtags.includes('#blacktechsky')) && !isHellthread
      })
      .map((create) => {
        // Create Blacksky posts in db
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
