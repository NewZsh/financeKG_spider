
- tyc_data/
    根据每个ID，有以下json文件：
    - 如果ID是“公司”类型（类型可以在`spider_progress.db`中获取，或者根据ID格式推断），有
        - base_info_{ID}.json
            - 爬天眼查网页的公司基础信息
            - 爬ID1的investments的时候，会连带返回被投资的公司基础信息，对比前者只缺少社会统一信用码等统一编码信息，该编码信息对后面实体的消歧、关联等应该有帮助，但是在缺少该信息的时候，也先把基础信息存下来
        - investments_{ID}.json
            - 该ID对外的一级投资信息
            - 每行一条投资信息，其中最重要的字段是name（被投公司名称）、id（被投公司ID）、percent（投资占比）
        - shareholder_{ID}.json 
            - TODO 