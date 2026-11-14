import json, random, time

random.seed(42)

# 词汇池：模拟新闻语料
subjects = ["量子计算", "人工智能", "芯片制造", "新能源汽车", "航天工程", "生物医药", "金融科技",
            "农业生产", "教育改革", "体育竞技", "城市规划", "环境保护", "能源转型", "文化遗产"]
verbs = ["推动了", "促进了", "影响了", "改变了", "重塑了", "加速了", "引领了", "支撑了"]
objects = ["产业升级", "技术创新", "经济增长", "社会发展", "国际合作", "人才培养", "数字化转型",
           "绿色发展", "消费需求", "供应链优化"]
regions = ["北京", "上海", "深圳", "杭州", "成都", "武汉", "西安", "南京", "广州", "苏州"]

# 特殊 planted 句子（ground truth 检索目标）
planted = {
    90001: "量子比特的相干时间是量子计算核心指标",
    90002: "人工智能大模型的训练需要海量算力支撑",
    90003: "航天工程的推进依赖新材料技术突破",
    90004: "新能源汽车的电池能量密度持续提升",
    90005: "生物医药的基因编辑技术日趋成熟",
}

rows = []
for i in range(1, 100001):
    if i in planted:
        text = planted[i]
    else:
        # 3-5 个随机子句，制造合理的中文文档
        n = random.randint(3, 5)
        text = "。".join(
            f"{random.choice(regions)}的{random.choice(subjects)}"
            f"{random.choice(verbs)}{random.choice(objects)}"
            for _ in range(n)
        )
    rows.append({"id": i, "text": text})

# 50 个查询：一半命中 planted 词，一半普通词
queries = (["量子比特", "量子 计算", "大模型 算力", "航天 新材料", "电池 能量密度", "基因编辑",
            "相干时间", "人工智能 训练", "新能源汽车 电池", "生物医药 基因"] * 3)[:30] + \
          ["产业升级", "技术创新 北京", "深圳 数字化转型", "绿色发展", "人才培养 杭州"] * 4

with open("/tmp/zh_corpus.json", "w") as f:
    json.dump(rows, f, ensure_ascii=False)
with open("/tmp/zh_queries.txt", "w") as f:
    f.write("\n".join(queries))
print(f"corpus: {len(rows)} rows, {len(queries)} queries")
print("sample planted:", planted[90001])
