val emails = """[\w\.-]+@[\w\.-]+""".r
val enrons = """CN=\w+>""".r
val emails2 = """[\w\.-]+@[\w\.-]+""".r


val email1 = "Alatorre Hector <Hector.X.Alatorre@Williams.com>, Alvarado Orlando <Orlando.Alvarado@Williams.com>, Ambler Margie <Margie.A.Ambler@Williams.com>"
val email2 = "Alonso, Tom </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Talonso>, Belden, Tim </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Tbelden>"
val email3 = "Walter White, Eleven Netflix, Rustin Cohle"
val email4 = "- *CFinch@PepcoEnergy.com <CFinch@PepcoEnergy.com>, - *Gthomas@energyusa.com <Gthomas@energyusa.com>,"

val x = for {
  m <- enrons.findAllIn(email2).matchData
  e <- m.subgroups
} yield e

val li1 = emails.findAllIn(email1).toList
val li2 = emails2.findAllIn(email4).toList
val li3 = emails.findAllIn(email3).toList
val li4 = enrons.findAllIn(email2).toList.map(x => x.substring(3,x.length-1))

(li1 ++ li2 ++ li3 ++ li4).mkString(",").filterNot(_ == " ")
(List("b","K") ++ List("a"," ")).filterNot(_.contains(" ")).mkString(",")
val str = "Hi, my - -name- is , . Enrico"
str.split(""""\\W+""")
