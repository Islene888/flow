import time
from state3.Engagement.Events import (
    Chat,
    Continue,
    Follow,
    New_Conversation,
    Message,
    Regen,
    Time_spent,
    View,
    Conversation_reset, click_chat_ratio, show_click_ratio, edit
)


def run_event(event_name, event_func, tag):
    print(f"\n🚀 开始执行 {event_name} 事件，标签：{tag}")
    start_time = time.time()
    try:
        event_func(tag)
        print(f"✅ {event_name} 事件执行完成，耗时：{round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ {event_name} 事件执行失败，错误信息：{e}")


def main(tag):
    print(f"\n🎬 【主流程启动】标签：{tag}\n")

    events = [
        ("Chat", Chat.main),
        ("Conversation", New_Conversation.main),
        ("Message", Message.main),
        ("Regen", Regen.main),
        ("TimeSpent", Time_spent.main),
        ("View", View.main),
        ("edit", edit.main),
        ("ConversationEnded", Conversation_reset.main),
        ("Continue", Continue.main),
        ("click_chat_ratio", click_chat_ratio.main),
        ("show_click_ratio", show_click_ratio.main),
        ("Follow", Follow.main)
    ]

    for event_name, event_func in events:
        run_event(event_name, event_func, tag)

    print("\n🎉 【所有事件处理完毕】")


if __name__ == "__main__":
    tag = "recommendation_mobile"  # 未来可以从外部传入或读取配置
    main(tag)
