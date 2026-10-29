/**
 * 一段对话的叫法。
 *
 * 并排几段对话时，几行都写着「新的对话」是没法指认的：说「左边那个」靠位置，位置
 * 一会就变了；说「对话 2」靠顺序，关掉一段之后序号全是错的。给每段一个自己带着的
 * 名字，是可以被念出来、被记住、也不随布局变化的。名字从会话 id 折出来，所以刷新
 * 之后还是同一个——它是这段对话的身份，不是这次渲染的编号。
 *
 * 名字取常见的英文人名，不取动物名，也不取某一国的古风名字：动物名念出来像绰号，
 * 古风名把读者限定在一种文化里。这一栏要被念出来指认——「Ada 那一段」在任何一种
 * 界面语言里都说得出口。
 *
 * 不再带十六进制后缀。后缀读起来像机器码，而这一栏是给人念的；名字多得足够，
 * 就不需要靠一个乱码尾巴去分开。
 */

/**
 * 三百多个常见的英文名，跨语区取：这一池子的意义是「同时开着的几段撞不上」。
 * 名字之间不要求首字母互不相同——几百个名字做不到这一点，也不需要：指认一段
 * 对话靠的是整串名字，不是第一个字母。
 */
const NAMES = [
  'Ada', 'Adam', 'Adrian', 'Aisha', 'Alan', 'Alberto', 'Alice',
  'Amara', 'Amir', 'Ana', 'Anders', 'Angela', 'Anika', 'Anna',
  'Aria', 'Arjun', 'Arthur', 'Asha', 'Ava', 'Ayana', 'Bea',
  'Bella', 'Ben', 'Bianca', 'Bina', 'Bjorn', 'Blake', 'Bonnie',
  'Boris', 'Bruce', 'Bruna', 'Bruno', 'Caleb', 'Camila', 'Cara',
  'Carlos', 'Carmen', 'Celia', 'Celine', 'Chen', 'Chloe', 'Clara',
  'Cleo', 'Cole', 'Connie', 'Cyrus', 'Daisy', 'Damian', 'Dana',
  'Daniel', 'Dara', 'Dario', 'David', 'Dawn', 'Delia', 'Dev',
  'Diego', 'Dilip', 'Dina', 'Dmitri', 'Dolores', 'Duarte', 'Eden',
  'Eduardo', 'Elena', 'Elias', 'Elif', 'Ella', 'Elliot', 'Emeka',
  'Emma', 'Emre', 'Eric', 'Erika', 'Esther', 'Ethan', 'Eva',
  'Ezra', 'Fabio', 'Farah', 'Farid', 'Fatima', 'Felix', 'Fernanda',
  'Finn', 'Fiona', 'Flora', 'Francis', 'Freya', 'Frida', 'Gabriel',
  'Gaia', 'Gauri', 'Gemma', 'George', 'Gina', 'Gita', 'Grace',
  'Greta', 'Guillermo', 'Gustavo', 'Gwen', 'Hana', 'Hani', 'Haruto',
  'Hassan', 'Hazel', 'Hector', 'Helen', 'Henry', 'Hilda', 'Hina',
  'Hiro', 'Hoda', 'Hugo', 'Ibrahim', 'Ida', 'Igor', 'Ilse',
  'Imani', 'Indira', 'Ines', 'Iris', 'Isaac', 'Isabel', 'Ivan',
  'Ivo', 'Jacob', 'Jada', 'James', 'Jane', 'Jasmin', 'Javier',
  'Jenna', 'Jerome', 'Jiao', 'Jin', 'Johanna', 'Jonas', 'Jorge',
  'Julia', 'Jun', 'Junaid', 'Kai', 'Kaito', 'Kamal', 'Kamila',
  'Kara', 'Karim', 'Katarina', 'Keiko', 'Kenji', 'Kiana', 'Kim',
  'Kira', 'Kiran', 'Klaus', 'Kofi', 'Kweku', 'Laila', 'Lana',
  'Lars', 'Laura', 'Leila', 'Lena', 'Leo', 'Liam', 'Lila',
  'Lin', 'Lucas', 'Lucia', 'Lucien', 'Luisa', 'Luka', 'Lydia',
  'Mabel', 'Mae', 'Magnus', 'Malik', 'Mara', 'Marco', 'Maria',
  'Marta', 'Mateo', 'Maya', 'Mei', 'Mel', 'Mia', 'Miguel',
  'Mila', 'Mina', 'Mira', 'Miriam', 'Mohammed', 'Musa', 'Nadia',
  'Nadine', 'Nam', 'Naomi', 'Nasir', 'Naveen', 'Neha', 'Neil',
  'Nia', 'Nils', 'Nina', 'Nisa', 'Noah', 'Noor', 'Nora',
  'Nuno', 'Ola', 'Olga', 'Omar', 'Oren', 'Orla', 'Oscar',
  'Osman', 'Otis', 'Owen', 'Oya', 'Pablo', 'Paloma', 'Pat',
  'Paula', 'Pedro', 'Peter', 'Petra', 'Pia', 'Pierre', 'Pilar',
  'Prasad', 'Prisha', 'Priya', 'Rachel', 'Rafael', 'Rahul', 'Rami',
  'Rania', 'Ravi', 'Rebecca', 'Renata', 'Reuben', 'Reza', 'Ricardo',
  'Rina', 'Rita', 'Rohan', 'Rosa', 'Ruby', 'Ruth', 'Saanvi',
  'Sade', 'Safiya', 'Salma', 'Sam', 'Samir', 'Sana', 'Sanjay',
  'Sara', 'Selene', 'Selin', 'Selma', 'Sergio', 'Shan', 'Shreya',
  'Simon', 'Sofia', 'Sonia', 'Soren', 'Stella', 'Sunil', 'Sven',
  'Tala', 'Talia', 'Tamara', 'Tara', 'Tarek', 'Tariq', 'Ted',
  'Tessa', 'Theo', 'Thomas', 'Tia', 'Tobias', 'Toma', 'Tomas',
  'Tulio', 'Tunde', 'Ugo', 'Uma', 'Umar', 'Una', 'Ursula',
  'Usha', 'Valentina', 'Valeria', 'Vania', 'Vera', 'Vesna', 'Victor',
  'Vikram', 'Vincenzo', 'Viola', 'Violet', 'Vivian', 'Viviana', 'Wafa',
  'Wale', 'Wanda', 'Wei', 'Werner', 'Will', 'Wilma', 'Wren',
  'Yael', 'Yara', 'Yasmin', 'Yohan', 'Yolanda', 'Yonas', 'Yuki',
  'Yuna', 'Yusra', 'Yusuf', 'Zain', 'Zane', 'Zara', 'Zelda',
  'Zeynep', 'Zhi', 'Zita', 'Zoe', 'Zoya', 'Zuri',
];

/** FNV-1a：一段短字符串折成一个稳定的数，不需要密码学性质。 */
function hash(id: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i += 1) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/**
 * 给现在开着的这几段对话各起一个名字。
 *
 * 分配按 id 排序走，不按打开顺序走：名字只取决于「现在开着哪几段」，谁先点开
 * 不影响结果，所以关掉又打开不会把名字洗一遍。两段恰好折到同一个名字上时，
 * 后一个往前走一格，拿最近的空位——同时开着的几段因此永远不重名，认错一段
 * 对话这种事不会因为概率低就发生。
 *
 * 代价写在明处：真要撞上，谁先占住那格决定谁改名，而「谁先」由 id 排序定，
 * 不由读者定。撞名在三百多个名字里本来就不多，撞上时改的是名字而不是身份——
 * 每段的标题与正文都还在原地。
 */
export function conversationNames(ids: readonly string[]): Map<string, string> {
  const taken = new Set<number>();
  const names = new Map<string, string>();
  for (const id of [...ids].sort()) {
    let slot = hash(id) % NAMES.length;
    let steps = 0;
    while (taken.has(slot) && steps < NAMES.length) {
      slot = (slot + 1) % NAMES.length;
      steps += 1;
    }
    taken.add(slot);
    names.set(id, NAMES[slot]);
  }
  return names;
}
