#### API 返回格式

- 搜索结果
```python
{
    "keyword": "CVTE",
    "total_companies": 42,
    "total_pages": 3,
    "company_ids": ["1", "2", ...],
    "timestamp": "2026-01-30T10:30:45.123456"
}
```

- 关键词列表 API
```python
{
    "exists": true,
    "keywords": ["CVTE", "百度", ...],
    "count": 10,
    "file_path": "/path/to/keywords.txt",
    "file_size": 1024,
    "last_modified": "2026-01-30T10:30:45.123456"
}
```


根据每个ID，`tyc_data/` 有以下json文件：

- 基础信息

    如果ID是“公司”类型（类型可以在`spider_progress.db`中获取，或者根据ID格式推断），有
        
    ```json
    {
        "id": "3478715717",
        "name": "视源电子股份有限公司",
        "legalRepresentative": "...",
        "registeredCapital": "...",
        "establishDate": "...",
        ...
    }
    ```
    位置：`data/tyc_data/base_info_{id}.json`

    - base_info 有独立的api接口，但是只能获取1个具体 ID 的公司信息，通常我们不知道其 ID，所以要用 search 
    - 根据关键字search的时候，是可以获取大于等于1个公司（至少一个精确命中）的base_info的，都存下来
    - 此外，爬ID1的investments的时候，会连带返回被投资的公司基础信息，对比前者只缺少社会统一信用码等统一编码信息，该编码信息对后面实体的消歧、关联等应该有帮助，但是在缺少该信息的时候，也先把基础信息存下来

- 投资信息 

    位置：`data/tyc_data/investments_{company_gid}.json`
        
    - investments_{ID}.json
        - 该ID对外的一级投资信息
        - 每行一条投资信息，其中最重要的字段是name（被投公司名称）、id（被投公司ID）、percent（投资占比）
    - shareholder_{ID}.json 
        - 每行一条股东关系记录，表示某个ID公司对应的一个股东（企业或个人）。样例如`shareholders_1007079667.json`包含的字段有：
            - `totalCapital`, `totalCapital4PC`, `totalCapitalUnit4PC` 等：公司的出资总额文本和拆分为数值/单位，用于计算占比。
            - `totalActualCapital*`：实际出资额，同上（有时为null）。
            - `totalCapitalChange`：出资变动次数。
            - `mainId` / `mainIdStr`：股东 ID。
            - `id`：本公司 ID。
            - `shareHolderType`：股东类型代码（例如2=企业法人、1=自然人等），`shareHolderTypeOnPage`为页面显示文字。
            - `shareHolderName`、`alias`：股东名称和别名。
            - `shareHolderNameId` / `shareHolderGid` / `shareHolderHid` / `shareHolderPid`：NameId通常等于后三者中的非null值，Pid代表这是一个人，Gid和Hid的区别我猜测是机构/公司类型的不同ID体系。
            - `percent`, `percent4Sort`, `percentChange`：持股比例及用于排序的数值。
            - `indirectBenefitShares`、`finalBenefitShares`：间接/最终受益股份，通常"-"或数字。
            - `logo`, `jigouLogo`、`productLogo`等：公司的标志链接。
            - `productId`、`productName`：关联产品信息。
            - `serviceType`、`serviceCount`等：天眼查服务相关字段。
            - `tags`：页面展示的标签数组，每个元素包含诸如`name`、`type`、`color`、`hoverNoticeContent`等，用于表示"控股股东"、"存续"、融资轮次等状态。
            - `capital`：出资记录列表，包含`amomon`（金额）、`payment`、`time`和`percent`。
            - `subscribedDate`、`latestCapitalTime`、`capitalTotal`：出资时间信息。
            - 还有诸多风险/状态字段（如`totalRisk`、`cluesOrRiskMsg`、`isShellCompany`等），根据天眼查页面返回的实时数据填充，可能为null。
            - 其余字段多为后台业务或UI使用，可以在有需要时查看示例。


- 特殊数据备忘（请结合 `neo4j_utils.py` 中的代码理解）：
    - investments_xxx.json 中，有可能有 tags 为空，经核查，公司存在，然而是HK公司，天眼查是有数据的，但是在对外投资关系中天眼查未将其基础数据作为 tags 返回