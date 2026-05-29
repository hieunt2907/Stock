export interface BaseEntity {
    id: number;
    description?: string;
    status?: number;
    note?: string;
    createdBy?: string;
    updatedBy?: string;
    deletedBy?: string;
    createdAt?: string;
    updatedAt?: string;
    deletedAt?: string;
}

export interface Spec extends BaseEntity {
    name: string;
    code: string;
    topics?: Topic[];
}

export interface Topic extends BaseEntity {
    name: string;
    code: string;
    spec?: Spec;
    vocabs?: Vocab[];
}

export interface Vocab extends BaseEntity {
    word: string;
    ipa: string;
    type: string;  // 'n' | 'v' | 'adj' | 'adv'
    audioUrl: string;
    meanings?: VocabMean[];
    examples?: VocabExam[];
    topics?: Topic[];
}

export interface VocabMean extends BaseEntity {
    meaningEn: string;
    meaningVi: string;
    vocab?: Vocab;
}

export interface VocabExam extends BaseEntity {
    sentenceEn: string;
    sentenceVi: string;
    audioUrl: string;
    vocab?: Vocab;
}

export interface BaseResponse<T> {
    code: string;
    message: string;
    data: T;
}

export interface Page<T> {
    content: T[];
    totalElements: number;
    totalPages: number;
    size: number;
    number: number;
    numberOfElements: number;
    first: boolean;
    last: boolean;
    empty: boolean;
}
