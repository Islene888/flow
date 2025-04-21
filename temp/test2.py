# 使用BERT抽取对话情感向量
from transformers import BertTokenizer, BertModel
tokenizer = BertTokenizer.from_pretrained('bert-base-chinese')
model = BertModel.from_pretrained('bert-base-chinese')

def extract_emotion(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()



# 添加在代码之后
if __name__ == "__main__":
    test_texts = [
        "今天天气真好，阳光明媚！",  # 正面情感
        "我很难过，因为考试没考好",  # 负面情感
        "请打开会议室的空调"        # 中性指令
    ]

    for text in test_texts:
        emotion_vector = extract_emotion(text)
        print(f"文本: {text}")
        print(f"情感向量维度: {emotion_vector.shape}")
        print(f"前5个特征值: {emotion_vector[0][:5]}\n")