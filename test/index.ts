import { query } from '../'

const tab1 = {
    name: 'table1',
    fields: [
        { name: 'field1', type: 'string', nullable: true },
        { name: 'field2', type: 'integer' }
    ]
} as const;

const tab2 = {
    name: 'table1',
    fields: [
        { name: 'field3', type: 'string' },
        { name: 'field4', type: 'integer' }
    ]
} as const;

(async () => {
    const res = await (
        query()
            .from(tab1, 't1')
            .join(tab2, 't2', ({ t1, t2 }) => t1.field2.equals(t2.field4))
            .select(({ t1, t2 }) => ({
                name: t1.field1,
                age: t2.field4
            }))
            .where(({ t1 }) => t1.field1.ifNull('').startsWith('blablabla'))
            .orderBy(({ t1 }) => [ t1.field1.desc(), t1.field2 ])
            .exec({ pageSize: 20, pageIndex: 0 })
    )
})()