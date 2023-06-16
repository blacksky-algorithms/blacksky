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
    const hardcoded_adds = ['did:plc:j4bko7yvzthmufkoxtzcoauh','did:plc:l4g436iw6lmd7ywrqz4lko5w','did:plc:mnkuzinn3jjjytuwdlw265ql','did:plc:f7gdbr6mkxcukdnjd7vdl4q4','did:plc:f4ctxz5nwkyfedkfvyxbtpgq','did:plc:7o55wjsyg2ylsmlr5to6gb67','did:plc:xum72mip7ti5niwqbgpvaqn4','did:plc:elelkrcnhv3adswxrkqkdt5k','did:plc:x4dmyp6bfmu3mshhx3fi4ko5','did:plc:m3ysxi4vufhxrg7syo55pt6r','did:plc:hxo5ss5y5p5wrhbxujcvru7w','did:plc:h5mzskhcrcjkf7vr4gadmf2c','did:plc:hsfzdq3icftr7vdp4d7krv4t','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:yddfcdhhinii6fj6hyulnzpk','did:plc:qlafttm5vzbjna7xltdfzdxh','did:plc:yl7wcldipsfnjdww2jg5mnrv','did:plc:tvt74yrjs4z4xvo3ov2vmk7f','did:plc:fpgknm3s36mthymrcjtgyerq','did:plc:vqs225hr64cjo5ookixhdqwr','did:plc:hqp33n2fqericzhtg6zprsog','did:plc:ccjaows3pzilatcha3n3fedj','did:plc:nsy2e4vupxndly4adhszebcy','did:plc:wcmdstkjsrghcsa6e76kocbs','did:plc:w6dfrjeqdshsmb2swhgmj4ju','did:plc:bei7c4ixr4dfmxunwnldt2xk','did:plc:4e3wxro75sds7cc2yfgnbnrc','did:plc:72g57t2jyqvy52w273mvfldl','did:plc:kpssseyzxoreqmqbh6sxbchm','did:plc:mvmfhonrfi2pwippivfcrvdw','did:plc:bkf2cmel54nxephykahosik2','did:plc:ahjc3qy354o4sqm2ynzw2aoe','did:plc:3bx2dysw3d3p54j4kzeridub','did:plc:fdgcujcsqzgxddcovzat23vb','did:plc:w4rlevqrhzxkc53tet2x6cs6','did:plc:wonrxml4kfrz7gnok6kciy6j','did:plc:nb6cbokqbfuq3gn4ifswjwam','did:plc:nzd4yp6wlmf3q5izext7vl5x','did:plc:h55dbmzzypr2hrxrjcjjcrq3','did:plc:vo7on74ip5lnlwtbm4aobim2','did:plc:fuknyynznmk26wg35ydjwcon','did:plc:ldpih6ykqs2wacnn3jug4ayf','did:plc:v2lyrftkaiu4yhy4ld4nt7qa','did:plc:avpi2kz2piugcemn7hdxx6h7','did:plc:5gt4sggyf3dhi5ee4lwebspz','did:plc:45k4ag25qqmksteyuo67yxpl','did:plc:5ypnozmulyjkgzpffkxwzbgn','did:plc:ieznolj4yeg7ezgcetdpqdy4','did:plc:57vlzz2egy6eqr4nksacmbht','did:plc:c4npsvzgb5yaeiu3ujg5nffy','did:plc:z45bsilgbevbobfnytjxcoll','did:plc:nyy6xonlicu4fdktafg3dv7s','did:plc:fcbcd54ypn2hkmfvyv2n5hae','did:plc:7gdi4pckwbqgnb3soazmhwrw','did:plc:lkg2uki5kkvjhyiehgai43li','did:plc:vtfgoubo76nku4q3wsx3iyw7','did:plc:k6mhxj4jl4hol635stqhkwib','did:plc:c6sulb24hiwth6nxdljsekao','did:plc:xjrrexzrqfkys3cd72chvpn2','did:plc:4dcp6k3kcop4jjjgssje5epl','did:plc:trhuguen3cmupmuai5lwy6fi','did:plc:bppz56h6f67t2tclkgq3h5oe','did:plc:haxycql755s4a7jl4hvmbfj3','did:plc:dh74sr7cc6s2psd4nacdzbpc','did:plc:e2ctbutx6kya6si4if5ngjmm','did:plc:yxepj65xiqw36ym547ikefba','did:plc:hy4mxyxvprjwltgj7o3dq47g','did:plc:rqknamd5zzwfekpqqsnylzqr','did:plc:whksjy5y7shgddk6o6qi3bff','did:plc:j5wdt346dbekcpjszpu4tvth','did:plc:3sczr6k6642eslsiervj272y','did:plc:xefaxesvgy7smyivbkvyojog','did:plc:p73662ybe643hnkpi2uyb4cl','did:plc:y742yf6jzv7rznyxsyn3nyo6','did:plc:lyylolptc5uxturmnetnhqfs','did:plc:5odb5x72xa7nltkjtf2gch2g','did:plc:75d33vgka7l7sc7zinztmgvn','did:plc:ekdqk6e3c5lkw3iwiigcy2gu','did:plc:kosvugygi3injgy4wwjtjsbs','did:plc:n2ylkhqdzjykr3mwwdpmhcsd','did:plc:6b7qj4tbzobgoduddyecfr22','did:plc:slo6mpsu7oytu6i5qt5cfeoo','did:plc:ciqqpyfnd2brriuxtjuv57xh','did:plc:p2ahkyueh77xh3p3fvyhabi7','did:plc:wjtbed3i2r7zki5a2bpuhurb','did:plc:2tjiu3ydbuaznxooclmxjetd','did:plc:yeoi4if4g6xvt7ycwlql2zjg','did:plc:tylusml32njglaw2lgiha35q','did:plc:wtonzz3kejo5dbrnshen5und','did:plc:oq5bzh5duusvc2san2apicvx','did:plc:djpchaxpyfqoc7ezu7xsmdj6','did:plc:iqx4itcsb2pwrtewjucxrnjl','did:plc:c54rnpmsf62o3wmqfqhwk3o2','did:plc:wven6ugrpgsdrxjuea3obkll','did:plc:ekcyikfp2mpymgtyzz7wkfuj','did:plc:zvyn4tctscn44fj4nrobupq5','did:plc:2vbrsdk6xr3iwerdkpvtsszv','did:plc:nvz6dcn63ftrzifnontes37m','did:plc:6u6uwdppytvyhgjvgucpjnig','did:plc:cewopa6cy2zuz742o4vmuvct','did:plc:ahptkqxcvmzzbblabrmfhi5v','did:plc:76il4ns5xbqbsqjwibjgcnsk','did:plc:5rkhafcde4xm4h5vpmo7ny74','did:plc:mglaxt4f53shpsu4u7nkmpyv','did:plc:3l6xjxyridfa7bj3wsramugy','did:plc:5lft52hjdtrvph4gcv6oyrfh','did:plc:qful3gp56v3r3p5byb3rmifs','did:plc:7mjbn64fshkrr6fr5uzeqzrs','did:plc:fhrogbenudj3j2cnpu5jfz2c','did:plc:kxbxcnefzqrxel3x2gmmkxza','did:plc:j2fkbwzibk6zopkpyoagzh62','did:plc:fvfbdqq3omplsfvzodbt6b5t','did:plc:bacph727xbk55zfu4tbxvzjp','did:plc:l5eyuhw7sx5nt42nfuzw45fy','did:plc:mukqkfmxqhfjklk3mkd2jbb3','did:plc:yxus52nvtcqpkyimdxmbry7u','did:plc:j2f2bfwdagwtyyvepo7ls34s','did:plc:pzqzwfhobi4ezaldfcnwe5wq','did:plc:rnpfw224gmkxpnhxa4px76ay','did:plc:hsk3oyzdd2nzojx7muyks5di','did:plc:bvhj2o6ufxdpue5nwdhokgig','did:plc:7gxo6vgwni6rqi2cz5wksoqs','did:plc:kiawmmanpegiqkkma4gvwszd','did:plc:bsajawy34wkiw2maftlrgarb','did:plc:fixvrvvpbve4pd3vu4czw67x','did:plc:jlodbpdbj5ubfyskvyrxkscp','did:plc:z4xos4auvltjbtjgvyptp256','did:plc:feibp2x4pgkmptrphoveeelr','did:plc:om5imrp42g3cxuttily2uelo','did:plc:kjklbsuepdppuppsfluytydl','did:plc:wag7fvz3qcgshtqh2fonv4t7','did:plc:xwyknsnacgf4n5al7njzomtk','did:plc:gwp56tuvyazjd7raw7n2wh6s','did:plc:g4ouxvgqjsxs66ywece6dkui','did:plc:ozmbb2vxbfoyucfqqo6bi24i','did:plc:mg7ux3oy6pjibuyo33cwh73b','did:plc:icheo67i5sweou3y7wkwvb4f','did:plc:tvqjxb6afrtnyzt7u3eufa2z','did:plc:gk3hyw347hfenh7jpunbdykl','did:plc:s2holzahcp3pc3ak7firz3nv','did:plc:y5mfbjfjhzohrhjvd6q3o2jb','did:plc:md6fp3oes2g2uovtdg25yv7d','did:plc:bcrh7ngtbce7zjwnbfsqqv2t','did:plc:rkmoupkmvr7jdm2nsncm5dyw','did:plc:p6ibr22u6gdedde47fawovgn','did:plc:dacd3jmqlhekeehsh24dq4is','did:plc:gpw24hg4amt44lq7e6tfqm2m','did:plc:mpomk4bvyopiyaqjzt4kfsm4','did:plc:ckfo6fu3limwhudbr3i4l2zb','did:plc:uwkfd2xu4eouvdzkrrr2jiiv','did:plc:5hpqwwpfpaofztm7ca6iuoxa','did:plc:ikc3vjln4cjtyuak5laua6jb','did:plc:3psakfmef5e273qvk6kwwybc','did:plc:rbm36gejadbapdqirgy255fj','did:plc:aixzcfgnblbmolzsp33254dd','did:plc:rcpeuhty7kxnvrt4slzvsxoa','did:plc:y3h4ot7vvqqo2it2awcw6yls','did:plc:6qpoegvd6voizk4vhujtlse3','did:plc:fi4j3j3r2gzk7gvthvbvh36v','did:plc:plffhva6yuckiv5izrgfrxeg','did:plc:dqixv6kuf4ygue7esufmbhn2','did:plc:tpwhumrwzl6wkvb4ecu5wui5','did:plc:6rzh4qv4b5ukqsdgq6hhpop7','did:plc:nvvk37oaxguncfiuvj6szusz','did:plc:pwklleswv5lrqz2cchzlkcsi','did:plc:jofrxz6u3xo77aayxc27cgjr','did:plc:lfuumxb35vetspd4twvz2fuk','did:plc:puudtpeyf4w5x4cewtk73hnk','did:plc:u7onp7b3qh3d6ra5jdjy72yd','did:plc:4cqxmkezm4xpqzt4nnqip6l5','did:plc:yeaejxh5hjcspi3z2ejgiotq','did:plc:3rmfgk7be6dik3y4vi3szmpf','did:plc:foopdc4wlkdds7kem6lzmfg2','did:plc:4szebb7osmz43cowwe2ohmxt','did:plc:33hbtedfnttb4mirzzdqeli4','did:plc:i32mnicewgt6rwvu5qr5f6kb','did:plc:g74woq5r62mgvhsnmw7anciw','did:plc:jl2rzh6ljh6mkdv36qdwkr7a','did:plc:ljlhzsoqytle2f4gdepkgibl','did:plc:w4agkeagfyjnzmozxh7bw667','did:plc:c5a74mtjyrbzsbbqnghwhjns','did:plc:crixkclwv2bktcbfjntkpizv','did:plc:7efcgpjzpmlyjztobimhguwb','did:plc:jccqavgwlv3xgtah2dteaaqz','did:plc:gtwsx6skq2skz5zosmogut5l','did:plc:xlkkp3onies3kgjvsxkqtg5o','did:plc:3mrdvulkjdvcoegxezrojpky','did:plc:4g4melrdxiwqmgeik55rkgx7','did:plc:giitoz2lzwfnkydo2sruq22r','did:plc:nxubfmvoxrhy7sfngtt6kxyt','did:plc:dscukyy5xqtocvs2ztoymcy4','did:plc:z4crqj2trvktx2fey74ujaiu','did:plc:fepeefxafhwa7yobbuwco2pl','did:plc:2g2u2vpta4l664ixri4oipdo','did:plc:2ylgbet7hkz6ntdh6eljnsri','did:plc:k5f642ejcpths5dehw7shm5t','did:plc:fvzkql2aqtbk7qmqjkoo2lv2','did:plc:wyds5pxwua6b4vvrgafe6vey','did:plc:iphamgxo3f44ibbkz2nsllml','did:plc:ikc3vjln4cjtyuak5laua6jb','did:plc:m6nfnqt2e4tzipp7aevhykpo','did:plc:qkulxlxgznoyw4vdy7nu2mof','did:plc:3w7ab2q3dlxscskrp7olqfd2','did:plc:v7gpwgulazsl4wjkrdwjis2l','did:plc:feywt2onatszum7kdyzklpib','did:plc:fsi2i4l5ezjc6c6x5bsm5qah','did:plc:kqltbfbrjk34e5qh75bsnwfp','did:plc:7ikl5b575otp6uxqsddmvuzh','did:plc:2aoh6c7x5ow7por3ykixprnc','did:plc:mbpk2abdxtk2j2lctwfpclvv','did:plc:wh3pv4fvnxycxhglk6x4ief5','did:plc:q55ibjjwdxfaocgwd65vejjs','did:plc:6q326nj54kdqqgjgu7ghm4hp','did:plc:rzkenmeipfagqf2d3sb4mmob','did:plc:2cfavpg33rorwcvmrhbgnerc','did:plc:jtlr72jtove7ot7cvr7t2amq','did:plc:bzdigjuzl2fff6eqhuguvybs','did:plc:a27o4wm3rf7djby43zyee4ld','did:plc:2xjrmxellugklnsiu7lu3ydz','did:plc:jl2rzh6ljh6mkdv36qdwkr7a','did:plc:wd6tv423ni7ww7w5k7xvkdio','did:plc:blb67egdpdjbwjtp32amb7s6','did:plc:h74svoxbiuppbgnuqfzgx5md','did:plc:xgjcudwc5yk4z2uex5v6e7bl','did:plc:ckw4lfdrvdobjajbl5egcoqx','did:plc:2noa3xfejgmgqjz3h2chts3y','did:plc:7dxte4hs6vqcabfg6birjsbp','did:plc:mjlhrk3pvgiwkj74onahzumw','did:plc:y3g57jaje4mdsunuucj4bpkt','did:plc:5mtyuhg6mnftrkjqmazj2gog','did:plc:bhgi444xnndrmuyxjbfxcsw5','did:plc:tvl4yuab6cojryajhuojpttd','did:plc:v7f3rcynajq4fcozaj7oe6mg','did:plc:vqltj2uwkuuj2ewsogwsoowr','did:plc:o44kuiswegv4gebtyamato3w','did:plc:v5gjfhvw4mssnizekc6vbaai','did:plc:ztkrqibfkvmwtfa64r25n4sv','did:plc:uhqbqnxlysbaxrfedpi6irwe','did:plc:cqkiqzdvvgjklp2ukqifo7rz','did:plc:mbwlzlpc5bxj62ezllqhm2sn','did:plc:realist5mcrfawzhtlnqfa6q','did:plc:gcfak3oclwsm6s5unf6hlynr','did:plc:i7vdeasvwafsklllgtkwf3ef','did:plc:p73fjep62lk56debqbvpvc5s','did:plc:i6q6ho2767angsnbtj7pfla6','did:plc:pwq7p7s4c2zum6c5tp7djmji','did:plc:2xslipmnvjxwl64x52l3zmdy','did:plc:uxn6n6mhzgtl63mxzugioaut','did:plc:7faob6hwrcc73mfhkfq735xc','did:plc:vzucxmaqcdizivyg5oersaik','did:plc:v7nr5j3vyovajng6y2p7efaq','did:plc:a3ls5giu467zlydjiewkdew4','did:plc:d6t5y2iro6bro6an4nky4gly','did:plc:wd6miy5cc4xhbzej6tdgk6qq','did:plc:4joatu57feamle4j7ufy7qpl','did:plc:wj5aqkvqa7jq73dy2zsrf56j','did:plc:nyefoihc5vfqmusbwyqtzwbs','did:plc:vcwaror26dgzd5fmordt7iir','did:plc:56gf3nosbe7ypa2p34n5lvuh','did:plc:5v72av4hhbwrr54zwdjjc2if','did:plc:caio7pgof5wycvv7jaihhizi','did:plc:oklokczybjnyuc235hru2bi2','did:plc:lzluvmi4zdrpgbplcxj63ake','did:plc:y5jt5fbkuojlxabj2hzdfh5b','did:plc:kckrl6l4tm7znzqphhridxnh','did:plc:hf4k7eomw4kzxgaewpias2h4','did:plc:wwdldsptf5zcpl3ydd32v2hq','did:plc:mqplt3ok3pc5h4y3ce3wz4o5','did:plc:mzfwluaitlbj2y57r6rbdeil','did:plc:iftd3vpa2k6u4l62smf4dqnd','did:plc:sk24aulz5xml6ejnijm5l7ux','did:plc:sp5paqzfmzlsxazgv7hrukda','did:plc:bfbwuaiwhlwkompmr2xlv33p','did:plc:j7dj4qiu525b7eaiqzlzjyj7','did:plc:auoafmsgezlsnarp7trhjj2i','did:plc:57wyfdc7uabe4rtwmhroqwda','did:plc:xcdw2kqtfoux47hktlh2jd4f','did:plc:xzrt7eiswrpr63sgusdhsbdl','did:plc:7lumgwomcezo4zjkyt7c45s3','did:plc:z2fviysxwatr2e3bdkvd6vvo','did:plc:7ncvsg7jsxxqr57x3uyz3p27','did:plc:xykdfumjlyildeuxfksure5u','did:plc:vdtmesvfxdkg2ix4sa2fqy54','did:plc:bwzr3mgrccggla5ym2vvbl6s','did:plc:frjjnhzvd27rhxqo7ykjvql5','did:plc:gnuwweqhp6khhe4unbz5ytr5','did:plc:tqcsw2et63tf2lkoa7bqof5e','did:plc:gzrecn3lwymykpn5qg47n2is','did:plc:7i3fhorekojhdjhkbln7q7gq','did:plc:v3vou5yc2354zjnfr6xtdq62','did:plc:6wnv6pgduaqplynbnm3okijw','did:plc:fzxda7agoncpqjz4mal2uiaw','did:plc:yitruju54bwyqgi44y2yyu4o','did:plc:s6tgdl72ftgzzgtkaxhvtbxi','did:plc:a63jv2b6iz3wz5l66h5ei7sk','did:plc:zxa3m6azo3b6yst5rgr7l4iq','did:plc:c55cxdfftqrual47dssiecp4','did:plc:uahgxoasxygs2iflcnkneztx','did:plc:wi4ymvz5h3j5ebykmylv6fos','did:plc:mbafliysnhbccg5uw6lcr4jp','did:plc:tsuuxbo44gua3hyx3d5nt7ar','did:plc:sjlocwe5syf3mcjgai4pnzf6','did:plc:i7ytwjkmktmc26s573vfbce5','did:plc:o35s22kyvagdwg3r4ppkdkpk','did:plc:rygq3lcmbtprq7zj7wxnmp22','did:plc:icj766ke3ragizbonckotsgb','did:plc:dvg5cqrp6caqaebmh54tyzxt','did:plc:vuj72vcid5o5eprj4eczb6xb','did:plc:iwvghqqyvawur2gjkfg4wg3f','did:plc:cyzpz6ogjbb4vdz476wrct43','did:plc:67vv4a35nhirbemmhmaefe4r','did:plc:6u67ylvymta2krbly4cofe2u','did:plc:cueb4eietocwhta3ouwij2wh']
    
    const hardcoded_removes = ['did:plc:buofnbcavecxm3kr6x5npusi','did:plc:le6rojzdlcqqbun4mwlo64wr','did:plc:y742yf6jzv7rznyxsyn3nyo6','did:plc:yz4djetpmxqhpkzabiaose6k','did:plc:5cn7tila5pqvqk7jbkgfz6hd']
    
    for (let add of hardcoded_adds) {
      blacksky.add(add)
    }
    for (let rm of hardcoded_removes) {
      blacksky.delete(rm)
    }

    console.log(`👨🏿‍💻 ${blacksky.size} individuals in the #BlackSky skyline!`)
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
