
## flink相关组件使用
相关目录涉及使用组件及说明
 - commonUtil
	广播流,kafkaSource,kafka发送文本工具
 - LoginFailDetect登录失败检测
    需求一:检测短时间内登录失败次数超限。通过ListState存储登录失败事件，当次数超限时判断时间最早登录失败时间时间和最晚登录失败事件时间范围是否在允许范围内。(有普通和cep两种方式)
 - MarkeAnalysis市场分析
   需求一：统计时间窗口内各渠道用户行为。通过keyBy渠道和行为字段，timewindow划定时间窗口，aggregateFunction,ProcessWindowFunction,统计结果。
   需求二：统计时间范围内广告点击量，并且过滤刷点击率事件。通过keyBy用户ID广告ID,KeyedProceeFunction中ValueState存储用户点错次数，当次数超上限输出侧输出流，再开时间窗口统计
 - NetworkFlowAnalysis网络流量分析
   需求一：统计网络用户访问量（数据量大时去重问题）。开时间窗后用Trigger让数据来一条触发窗口计算并情况窗口，用reids根据时间窗口Endtime做key存bitmap,根据自定义布隆过滤器hashcode更新redis bitmap,判断用户是否重复，不重复则计入总数。
   需求二：统计所有页面访问量（数据倾斜）。开窗前给对象加入一个随机数的key，keyBy这个随机数解决数据倾斜。
 - orderPayDetect订单支付检测
   需求一：订单超时告警。使用followedBy within方法创建带时间限制的pattern。
   需求二：订单支付对账。使用connect或join算子两种方式将订单流和支付流结合并判断账单是否正确。

