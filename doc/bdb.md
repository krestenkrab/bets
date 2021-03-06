Module bdb
==========


<h1>Module bdb</h1>

* [Function Index](#index)
* [Function Details](#functions)






<h2><a name="index">Function Index</a></h2>



<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#current-1">current/1</a></td><td></td></tr><tr><td valign="top"><a href="#fold-3">fold/3</a></td><td></td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td>
fold/4 folds over all elements with a given prefix.</td></tr><tr><td valign="top"><a href="#lookup-2">lookup/2</a></td><td></td></tr><tr><td valign="top"><a href="#transaction-2">transaction/2</a></td><td></td></tr><tr><td valign="top"><a href="#transaction-3">transaction/3</a></td><td></td></tr><tr><td valign="top"><a href="#transactional-2">transactional/2</a></td><td>
Run <code>Fun</code> transactionally on <code>DB</code>; either in a new transaction
or in the current if there is such.</td></tr><tr><td valign="top"><a href="#with_cursor-2">with_cursor/2</a></td><td></td></tr></table>




<h2><a name="functions">Function Details</a></h2>


<a name="current-1"></a>

<h3>current/1</h3>





`current(Db) -> any()`

<a name="fold-3"></a>

<h3>fold/3</h3>





`fold(Fun, Acc, Db) -> any()`

<a name="fold-4"></a>

<h3>fold/4</h3>





`fold(Fun::['fun'](#type-fun)(fun((binary(), Acc) -> Acc)), Acc, Prefix::binary(), DB::#db{}) -> Acc`
<br></br>





fold/4 folds over all elements with a given prefix<a name="lookup-2"></a>

<h3>lookup/2</h3>





`lookup(Db, BinKey) -> any()`

<a name="transaction-2"></a>

<h3>transaction/2</h3>





`transaction(DB, Fun) -> any()`

<a name="transaction-3"></a>

<h3>transaction/3</h3>





`transaction(DB, Fun, Retries) -> any()`

<a name="transactional-2"></a>

<h3>transactional/2</h3>





`transactional(DB, Fun) -> any()`




Run `Fun` transactionally on `DB`; either in a new transaction
or in the current if there is such.<a name="with_cursor-2"></a>

<h3>with_cursor/2</h3>





`with_cursor(Db, Fun) -> any()`



_Generated by EDoc, Apr 4 2011, 00:33:15._